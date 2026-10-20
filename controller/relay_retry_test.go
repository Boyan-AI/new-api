package controller

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newCPARecoveryFixture 显式初始化重试配置和托管渠道，不依赖本机服务或数据库。
func newCPARecoveryFixture(t *testing.T) (*gin.Context, *model.Channel, *types.NewAPIError) {
	t.Helper()
	previousRetries := common.RetryTimes
	previousStatuses := operation_setting.AutomaticRetryStatusCodesToString()
	common.RetryTimes = 2
	require.NoError(t, operation_setting.AutomaticRetryStatusCodesFromString("503"))
	t.Cleanup(func() {
		common.RetryTimes = previousRetries
		require.NoError(t, operation_setting.AutomaticRetryStatusCodesFromString(previousStatuses))
	})
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	baseURL := "http://127.0.0.1:8317"
	channel := &model.Channel{Name: "Codex Auth CPA", BaseURL: &baseURL}
	// 通过正式错误解析器读取真实 CPA 响应格式，验证恢复判断与线上协议一致。
	upstreamErr := service.RelayErrorHandler(c.Request.Context(), &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"auth_unavailable: no auth available (providers=codex, model=gpt-6.1-sol)","type":"server_error","code":"internal_server_error"}}`)),
	}, false)
	require.NotNil(t, upstreamErr)
	return c, channel, upstreamErr
}

// TestCPARecoveryResumesAfterTransientFailure 验证短暂失败后可继续原请求，成功即停止等待。
func TestCPARecoveryResumesAfterTransientFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, channel, upstreamErr := newCPARecoveryFixture(t)
		var recovery cpaRecovery
		require.True(t, recovery.wait(c, channel, upstreamErr))
		require.True(t, recovery.wait(c, channel, upstreamErr))
		assert.False(t, recovery.wait(c, channel, nil), "successful upstream responses must stop recovery")
	})
}

// TestCPARecoveryExhaustsBudget 用虚拟时间验证持续故障最多占用一分钟恢复预算。
func TestCPARecoveryExhaustsBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, channel, upstreamErr := newCPARecoveryFixture(t)
		start := time.Now()
		var recovery cpaRecovery
		attempts := 0
		for recovery.wait(c, channel, upstreamErr) {
			attempts++
		}
		assert.Equal(t, time.Minute, time.Since(start))
		assert.Equal(t, 6, attempts, "persistent failure must not trigger a busy retry loop")
		assert.False(t, recovery.wait(c, channel, upstreamErr), "an exhausted budget must not restart")
	})
}

// TestCPARecoveryCancellation 验证客户端取消会唤醒正在等待的重试，无需等待定时器。
func TestCPARecoveryCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, channel, upstreamErr := newCPARecoveryFixture(t)
		ctx, cancel := context.WithCancel(c.Request.Context())
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		var recovery cpaRecovery
		result := make(chan bool, 1)
		go func() { result <- recovery.wait(c, channel, upstreamErr) }()
		synctest.Wait()
		cancel()
		require.False(t, <-result)
		assert.False(t, shouldRetry(c, upstreamErr, 1))
	})
}

// TestCPARecoverySafetyBoundaries 验证非目标故障、用户约束和已提交响应都不能触发重放。
func TestCPARecoverySafetyBoundaries(t *testing.T) {
	tests := []struct {
		name   string
		modify func(*gin.Context, *model.Channel, *types.NewAPIError)
	}{
		{"unauthorized", func(_ *gin.Context, _ *model.Channel, e *types.NewAPIError) { e.StatusCode = 401 }},
		{"forbidden", func(_ *gin.Context, _ *model.Channel, e *types.NewAPIError) { e.StatusCode = 403 }},
		{"generic unavailable", func(_ *gin.Context, _ *model.Channel, e *types.NewAPIError) {
			e.Err = errors.New("upstream unavailable")
		}},
		{"remote channel", func(_ *gin.Context, ch *model.Channel, _ *types.NewAPIError) {
			u := "https://example.com"
			ch.BaseURL = &u
		}},
		{"user channel", func(_ *gin.Context, ch *model.Channel, _ *types.NewAPIError) { ch.Name = "User channel" }},
		{"other endpoint", func(c *gin.Context, _ *model.Channel, _ *types.NewAPIError) {
			c.Request.URL.Path = "/v1/images/generations"
		}},
		{"disabled retries", func(_ *gin.Context, _ *model.Channel, _ *types.NewAPIError) { common.RetryTimes = 0 }},
		{"excluded status", func(_ *gin.Context, _ *model.Channel, _ *types.NewAPIError) {
			operation_setting.AutomaticRetryStatusCodeRanges = nil
		}},
		{"explicit skip retry", func(_ *gin.Context, _ *model.Channel, e *types.NewAPIError) {
			types.ErrOptionWithSkipRetry()(e)
		}},
		{"single attempt pin", func(c *gin.Context, _ *model.Channel, _ *types.NewAPIError) {
			service.GetChannelConstraints(c).AddPin(dto.ChannelPin{ChannelId: 1, Source: dto.PinSourceToken, Rank: dto.PinRankToken, RetryMode: dto.PinRetrySingleAttempt})
		}},
		{"stream content emitted", func(c *gin.Context, _ *model.Channel, _ *types.NewAPIError) {
			_, _ = c.Writer.Write([]byte("data: started\n\n"))
		}},
		{"headers flushed", func(c *gin.Context, _ *model.Channel, _ *types.NewAPIError) { c.Writer.Flush() }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, channel, upstreamErr := newCPARecoveryFixture(t)
			tt.modify(c, channel, upstreamErr)
			var recovery cpaRecovery
			assert.False(t, recovery.wait(c, channel, upstreamErr))
			if c.Writer.Written() {
				assert.False(t, shouldRetry(c, upstreamErr, 1), "ordinary retries must also stop after response commitment")
			}
		})
	}
}
