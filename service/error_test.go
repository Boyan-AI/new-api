package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResetStatusCode(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name             string
		statusCode       int
		statusCodeConfig string
		expectedCode     int
	}{
		{
			name:             "map string value",
			statusCode:       429,
			statusCodeConfig: `{"429":"503"}`,
			expectedCode:     503,
		},
		{
			name:             "map int value",
			statusCode:       429,
			statusCodeConfig: `{"429":503}`,
			expectedCode:     503,
		},
		{
			name:             "skip invalid string value",
			statusCode:       429,
			statusCodeConfig: `{"429":"bad-code"}`,
			expectedCode:     429,
		},
		{
			name:             "skip status code 200",
			statusCode:       200,
			statusCodeConfig: `{"200":503}`,
			expectedCode:     200,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			newAPIError := &types.NewAPIError{
				StatusCode: tc.statusCode,
			}
			ResetStatusCode(newAPIError, tc.statusCodeConfig)
			require.Equal(t, tc.expectedCode, newAPIError.StatusCode)
		})
	}
}

func TestRelayErrorHandlerTruncatesInvalidJSONBodyInLog(t *testing.T) {
	withDebugEnabled(t, false)

	body := strings.Repeat("b", common.LocalLogContentLimit+256)
	var logBuffer bytes.Buffer

	common.LogWriterMu.Lock()
	oldWriter := gin.DefaultErrorWriter
	gin.DefaultErrorWriter = &logBuffer
	common.LogWriterMu.Unlock()
	t.Cleanup(func() {
		common.LogWriterMu.Lock()
		gin.DefaultErrorWriter = oldWriter
		common.LogWriterMu.Unlock()
	})

	resp := &http.Response{
		StatusCode: http.StatusInternalServerError,
		Body:       io.NopCloser(strings.NewReader(body)),
	}

	newAPIError := RelayErrorHandler(context.Background(), resp, false)

	require.NotNil(t, newAPIError)
	require.Equal(t, "bad response status code 500", newAPIError.Error())
	require.Contains(t, logBuffer.String(), "[truncated")
	require.Contains(t, logBuffer.String(), fmt.Sprintf("original_length=%d", len(body)))
	require.NotContains(t, logBuffer.String(), strings.Repeat("b", common.LocalLogContentLimit+1))
}

func TestRelayErrorHandlerKeepsStructuredErrorMessage(t *testing.T) {
	message := strings.Repeat("c", common.LocalLogContentLimit+256)
	body := `{"message":"` + message + `"}`
	resp := &http.Response{
		StatusCode: http.StatusInternalServerError,
		Body:       io.NopCloser(strings.NewReader(body)),
	}

	newAPIError := RelayErrorHandler(context.Background(), resp, false)

	require.NotNil(t, newAPIError)
	require.Equal(t, message, newAPIError.Error())
}

func TestRelayErrorHandlerKeepsOpenAIErrorMessage(t *testing.T) {
	message := strings.Repeat("d", common.LocalLogContentLimit+256)
	body := `{"error":{"message":"` + message + `","type":"server_error","code":"server_error"}}`
	resp := &http.Response{
		StatusCode: http.StatusInternalServerError,
		Body:       io.NopCloser(strings.NewReader(body)),
	}

	newAPIError := RelayErrorHandler(context.Background(), resp, false)

	require.NotNil(t, newAPIError)
	require.Equal(t, message, newAPIError.Error())
}

func TestRelayErrorHandlerKeepsInvalidJSONBodyInDebugLog(t *testing.T) {
	withDebugEnabled(t, true)

	body := strings.Repeat("e", common.LocalLogContentLimit+256)
	var logBuffer bytes.Buffer

	common.LogWriterMu.Lock()
	oldWriter := gin.DefaultErrorWriter
	gin.DefaultErrorWriter = &logBuffer
	common.LogWriterMu.Unlock()
	t.Cleanup(func() {
		common.LogWriterMu.Lock()
		gin.DefaultErrorWriter = oldWriter
		common.LogWriterMu.Unlock()
	})

	resp := &http.Response{
		StatusCode: http.StatusInternalServerError,
		Body:       io.NopCloser(strings.NewReader(body)),
	}

	newAPIError := RelayErrorHandler(context.Background(), resp, false)

	require.NotNil(t, newAPIError)
	require.NotContains(t, logBuffer.String(), "[truncated")
	require.Contains(t, logBuffer.String(), body)
}

func withDebugEnabled(t *testing.T, enabled bool) {
	t.Helper()

	oldDebug := common.DebugEnabled
	common.DebugEnabled = enabled
	t.Cleanup(func() {
		common.DebugEnabled = oldDebug
	})
}

// TestRelayErrorHandlerPreservesRetryAfter 验证不同错误解析分支保留协议提示而不改变错误正文。
func TestRelayErrorHandlerPreservesRetryAfter(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"openai", `{"error":{"message":"temporarily unavailable","type":"server_error","code":"server_error"}}`},
		{"message", `{"message":"temporarily unavailable"}`},
		{"invalid json", "temporarily unavailable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				resp := &http.Response{
					StatusCode: http.StatusServiceUnavailable,
					Header:     http.Header{"Retry-After": []string{"12"}},
					Body:       io.NopCloser(strings.NewReader(tt.body)),
				}
				apiErr := RelayErrorHandler(context.Background(), resp, false)
				require.NotNil(t, apiErr)
				assert.Equal(t, 12*time.Second, UpstreamRetryDelay(apiErr))
				assert.NotContains(t, apiErr.Error(), "Retry-After")
				ResetStatusCode(apiErr, `{"503":429}`)
				assert.Equal(t, http.StatusTooManyRequests, apiErr.StatusCode)
				time.Sleep(5 * time.Second)
				assert.Equal(t, 7*time.Second, UpstreamRetryDelay(apiErr))
				time.Sleep(8 * time.Second)
				assert.Zero(t, UpstreamRetryDelay(apiErr))
			})
		})
	}
}

// TestParseUpstreamRetryAfter 验证标准日期、非法提示和极大秒数都具有安全可预测的语义。
func TestParseUpstreamRetryAfter(t *testing.T) {
	receivedAt := time.Date(2026, 10, 22, 6, 0, 0, 0, time.UTC)
	tests := []struct {
		value string
		valid bool
		delay time.Duration
	}{
		{"0", true, 0},
		{" 12 ", true, 12 * time.Second},
		{receivedAt.Add(20 * time.Second).Format(http.TimeFormat), true, 20 * time.Second},
		{receivedAt.Add(-time.Second).Format(http.TimeFormat), true, -time.Second},
		{"-1", false, 0},
		{"1.5", false, 0},
		{"not a date", false, 0},
		{"", false, 0},
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			retryAt, valid := parseUpstreamRetryAfter(tt.value, receivedAt)
			assert.Equal(t, tt.valid, valid)
			if valid {
				assert.Equal(t, tt.delay, retryAt.Sub(receivedAt))
			}
		})
	}
	for _, value := range []string{"18446744073709551615", "99999999999999999999999999999999"} {
		retryAt, valid := parseUpstreamRetryAfter(value, receivedAt)
		require.True(t, valid)
		assert.True(t, retryAt.After(receivedAt.Add(24*time.Hour)), "overflow must not turn a long server wait into an immediate retry")
	}
}

// failedUpstreamErrorBody 模拟失败的上游错误体，记录正式解析器是否释放响应资源。
type failedUpstreamErrorBody struct {
	closed bool
}

// Read 返回上游断流错误，验证解析失败的清理分支。
func (body *failedUpstreamErrorBody) Read(_ []byte) (int, error) {
	return 0, errors.New("upstream body read failed")
}

// Close 标记资源释放，避免读取失败遗留取消回调和连接。
func (body *failedUpstreamErrorBody) Close() error {
	body.closed = true
	return nil
}

// TestRelayErrorHandlerClosesUnreadableBody 验证错误体读取失败仍关闭响应并保留重试提示。
func TestRelayErrorHandlerClosesUnreadableBody(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		body := &failedUpstreamErrorBody{}
		apiErr := RelayErrorHandler(context.Background(), &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Header:     http.Header{"Retry-After": []string{"12"}},
			Body:       body,
		}, false)
		require.NotNil(t, apiErr)
		assert.True(t, body.closed)
		assert.Equal(t, 12*time.Second, UpstreamRetryDelay(apiErr))
	})
}
