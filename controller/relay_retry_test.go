package controller

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/i18n"
	"github.com/QuantumNous/new-api/middleware"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/QuantumNous/new-api/setting/ratio_setting"
	"github.com/bytedance/gopkg/util/gopool"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// newChannelRecoveryFixture 显式初始化重试配置和普通上游错误，不依赖真实服务或数据库。
func newChannelRecoveryFixture(t *testing.T) (*gin.Context, *types.NewAPIError) {
	t.Helper()
	previousRetries := common.RetryTimes
	previousStatuses := operation_setting.AutomaticRetryStatusCodesToString()
	common.RetryTimes = 2
	require.NoError(t, operation_setting.AutomaticRetryStatusCodesFromString("408,429,500-599"))
	t.Cleanup(func() {
		common.RetryTimes = previousRetries
		require.NoError(t, operation_setting.AutomaticRetryStatusCodesFromString(previousStatuses))
	})
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	// 通过正式解析器处理普通非 CPA 错误，避免测试仅覆盖历史专属文案。
	upstreamErr := service.RelayErrorHandler(c.Request.Context(), &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"upstream temporarily unavailable","type":"server_error","code":"server_error"}}`)),
	}, false)
	require.NotNil(t, upstreamErr)
	return c, upstreamErr
}

// TestChannelRecoveryPreservesEveryModel 验证模型和供应商文案不影响恢复，成功后立即结束。
func TestChannelRecoveryPreservesEveryModel(t *testing.T) {
	for _, modelName := range []string{"gpt-6.1-sol", "gpt-6-astra", "gpt-5.6-sol", "custom-model"} {
		t.Run(modelName, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, upstreamErr := newChannelRecoveryFixture(t)
				c.Set("original_model", modelName)
				var recovery channelRecovery
				defer recovery.close()
				require.True(t, recovery.beginAttempt(c))
				require.True(t, recovery.wait(c, upstreamErr))
				require.True(t, recovery.beginAttempt(c))
				assert.False(t, recovery.wait(c, nil), "successful upstream responses must stop recovery")
				assert.Equal(t, modelName, c.GetString("original_model"))
			})
		})
	}
}

// TestChannelRecoveryCapsAttemptsAndBackoff 验证持续故障只请求五次，且退避范围没有忙重试。
func TestChannelRecoveryCapsAttemptsAndBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, upstreamErr := newChannelRecoveryFixture(t)
		start := time.Now()
		var recovery channelRecovery
		defer recovery.close()
		attempts := 0
		for recovery.beginAttempt(c) {
			attempts++
			waitStart := time.Now()
			if !recovery.wait(c, upstreamErr) {
				break
			}
			maxDelay := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 15 * time.Second}[attempts-1]
			assert.GreaterOrEqual(t, time.Since(waitStart), maxDelay/2)
			assert.LessOrEqual(t, time.Since(waitStart), maxDelay)
		}
		assert.Equal(t, 5, attempts)
		assert.Less(t, time.Since(start), time.Minute)
		assert.False(t, recovery.beginAttempt(c), "an exhausted attempt limit must not restart")
		assert.False(t, recovery.wait(c, upstreamErr))
	})
}

// TestChannelRecoveryBudgetIncludesRequests 验证慢恢复请求消耗统一预算，截止后不能再请求。
func TestChannelRecoveryBudgetIncludesRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, upstreamErr := newChannelRecoveryFixture(t)
		var recovery channelRecovery
		defer recovery.close()
		require.True(t, recovery.beginAttempt(c))
		start := time.Now()
		require.True(t, recovery.wait(c, upstreamErr))
		require.True(t, recovery.beginAttempt(c))
		<-c.Request.Context().Done()
		assert.Equal(t, time.Minute, time.Since(start))
		assert.ErrorIs(t, context.Cause(c.Request.Context()), context.DeadlineExceeded)
		assert.False(t, recovery.wait(c, upstreamErr))
		assert.False(t, recovery.beginAttempt(c))
	})
}

// TestChannelRecoveryHonorsRetryAfter 验证协议提示是等待下限，超过预算时不提前重试。
func TestChannelRecoveryHonorsRetryAfter(t *testing.T) {
	for _, seconds := range []string{"12", "120"} {
		t.Run(seconds, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, _ := newChannelRecoveryFixture(t)
				upstreamErr := service.RelayErrorHandler(c.Request.Context(), &http.Response{
					StatusCode: http.StatusServiceUnavailable,
					Header:     http.Header{"Retry-After": []string{seconds}},
					Body:       io.NopCloser(strings.NewReader(`{"message":"temporarily unavailable"}`)),
				}, false)
				var recovery channelRecovery
				defer recovery.close()
				require.True(t, recovery.beginAttempt(c))
				start := time.Now()
				if seconds == "12" {
					require.True(t, recovery.wait(c, upstreamErr))
					assert.Equal(t, 12*time.Second, time.Since(start))
				} else {
					assert.False(t, recovery.wait(c, upstreamErr))
					assert.Zero(t, time.Since(start), "a server wait beyond the budget must return without an early retry")
				}
			})
		})
	}
}

// TestChannelRecoveryCancellation 验证客户端取消会唤醒等待，不需要等退避定时器。
func TestChannelRecoveryCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, upstreamErr := newChannelRecoveryFixture(t)
		ctx, cancel := context.WithCancel(c.Request.Context())
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		var recovery channelRecovery
		defer recovery.close()
		require.True(t, recovery.beginAttempt(c))
		result := make(chan bool, 1)
		go func() { result <- recovery.wait(c, upstreamErr) }()
		synctest.Wait()
		cancel()
		require.False(t, <-result)
		assert.False(t, shouldRetry(c, upstreamErr, 1))
		assert.False(t, recovery.beginAttempt(c))
	})
}

// TestChannelRecoverySafetyBoundaries 验证永久错误、配置约束和已提交响应不能扩大恢复或重放。
func TestChannelRecoverySafetyBoundaries(t *testing.T) {
	tests := []struct {
		name   string
		modify func(*gin.Context, *types.NewAPIError)
	}{
		{"unauthorized", func(_ *gin.Context, e *types.NewAPIError) { e.StatusCode = 401 }},
		{"forbidden", func(_ *gin.Context, e *types.NewAPIError) { e.StatusCode = 403 }},
		{"bad request", func(_ *gin.Context, e *types.NewAPIError) { e.StatusCode = 400 }},
		{"no channel", func(_ *gin.Context, e *types.NewAPIError) {
			*e = *types.NewErrorWithStatusCode(errors.New("no available channel"), types.ErrorCodeGetChannelFailed, 503)
		}},
		{"model not configured", func(_ *gin.Context, e *types.NewAPIError) {
			*e = *types.NewErrorWithStatusCode(errors.New("model not configured"), types.ErrorCodeModelNotFound, 503)
		}},
		{"disabled retries", func(_ *gin.Context, _ *types.NewAPIError) { common.RetryTimes = 0 }},
		{"excluded status", func(_ *gin.Context, _ *types.NewAPIError) {
			operation_setting.AutomaticRetryStatusCodeRanges = nil
		}},
		{"explicit skip retry", func(_ *gin.Context, e *types.NewAPIError) {
			types.ErrOptionWithSkipRetry()(e)
		}},
		{"single attempt pin", func(c *gin.Context, _ *types.NewAPIError) {
			service.GetChannelConstraints(c).AddPin(dto.ChannelPin{ChannelId: 1, Source: dto.PinSourceToken, Rank: dto.PinRankToken, RetryMode: dto.PinRetrySingleAttempt})
		}},
		{"stream content emitted", func(c *gin.Context, _ *types.NewAPIError) {
			_, _ = c.Writer.Write([]byte("data: started\n\n"))
		}},
		{"headers flushed", func(c *gin.Context, _ *types.NewAPIError) { c.Writer.Flush() }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, upstreamErr := newChannelRecoveryFixture(t)
			tt.modify(c, upstreamErr)
			var recovery channelRecovery
			defer recovery.close()
			assert.False(t, recovery.wait(c, upstreamErr))
			if c.Writer.Written() {
				assert.False(t, shouldRetry(c, upstreamErr, 1), "ordinary retries must also stop after response commitment")
			}
		})
	}
}

// runRelayRecoveryInIsolatedProcess 隔离原生后台任务和全局配置，避免整链路测试污染其他用例。
func runRelayRecoveryInIsolatedProcess(t *testing.T) bool {
	t.Helper()
	if os.Getenv("NEW_API_RELAY_RECOVERY_TEST_PROCESS") == t.Name() {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	process := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$", "-test.timeout=70s")
	process.Env = append(os.Environ(), "NEW_API_RELAY_RECOVERY_TEST_PROCESS="+t.Name())
	output, err := process.CombinedOutput()
	require.NoError(t, err, "isolated relay recovery test failed: %s", output)
	return true
}

// newRelayRecoveryFixture 使用独立进程内的隔离数据库和测试定价初始化正式分发链路。
func newRelayRecoveryFixture(t *testing.T, modelName string) *gorm.DB {
	t.Helper()
	_, _ = newChannelRecoveryFixture(t)
	previousDB, previousLogDB := model.DB, model.LOG_DB
	previousMemoryCache, previousRedis := common.MemoryCacheEnabled, common.RedisEnabled
	previousErrorLog, previousConsumeLog := constant.ErrorLogEnabled, common.LogConsumeEnabled
	previousCountToken := constant.CountToken
	previousStreamingTimeout := constant.StreamingTimeout
	previousQuota := *operation_setting.GetQuotaSetting()
	previousPing := *operation_setting.GetGeneralSetting()
	previousMainType, previousLogType := common.MainDatabaseType(), common.LogDatabaseType()
	previousRatios := ratio_setting.ModelRatio2JSONString()
	common.MemoryCacheEnabled, common.RedisEnabled = false, false
	constant.ErrorLogEnabled, common.LogConsumeEnabled, constant.CountToken = false, false, false
	constant.StreamingTimeout = 300
	operation_setting.GetQuotaSetting().EnableFreeModelPreConsume = false
	operation_setting.GetGeneralSetting().PingIntervalEnabled = false
	common.SetDatabaseTypes(common.DatabaseTypeSQLite, common.DatabaseTypeSQLite)
	initModelListColumnNames(t)
	database, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := database.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	require.NoError(t, database.AutoMigrate(&model.Channel{}, &model.Ability{}, &model.User{}))
	model.DB, model.LOG_DB = database, database
	ratios, err := common.Marshal(map[string]float64{modelName: 0})
	require.NoError(t, err)
	require.NoError(t, ratio_setting.UpdateModelRatioByJSONString(string(ratios)))
	service.InitHttpClient()
	require.NoError(t, i18n.Init())
	t.Cleanup(func() {
		// 独立进程仅启动本用例的指标任务，等待完成后再恢复测试全局配置。
		require.Eventually(t, func() bool { return gopool.WorkerCount() == 0 }, time.Second, time.Millisecond,
			"native relay metric tasks must finish before restoring fixture globals")
		model.DB, model.LOG_DB = previousDB, previousLogDB
		common.MemoryCacheEnabled, common.RedisEnabled = previousMemoryCache, previousRedis
		constant.ErrorLogEnabled, common.LogConsumeEnabled, constant.CountToken = previousErrorLog, previousConsumeLog, previousCountToken
		constant.StreamingTimeout = previousStreamingTimeout
		*operation_setting.GetQuotaSetting() = previousQuota
		*operation_setting.GetGeneralSetting() = previousPing
		common.SetDatabaseTypes(previousMainType, previousLogType)
		require.NoError(t, ratio_setting.UpdateModelRatioByJSONString(previousRatios))
		require.NoError(t, sqlDB.Close())
	})
	return database
}

// newRelayRecoveryRequest 复用真实 distributor 和 Relay，测试不绕过模型及渠道选择。
func newRelayRecoveryRequest(t *testing.T, modelName string, stream bool, extraMiddleware ...gin.HandlerFunc) (*gin.Engine, *http.Request) {
	t.Helper()
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Request.Header.Set("Accept-Language", "en")
		common.SetContextKey(c, constant.ContextKeyUsingGroup, "default")
		common.SetContextKey(c, constant.ContextKeyUserGroup, "default")
		common.SetContextKey(c, constant.ContextKeyTokenGroup, "default")
		c.Next()
	})
	router.Use(extraMiddleware...)
	router.POST("/v1/responses", middleware.Distribute(), func(c *gin.Context) { Relay(c, types.RelayFormatOpenAIResponses) })
	body, err := common.Marshal(map[string]any{
		"model": modelName, "input": "Reply OK", "stream": stream, "store": false,
	})
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	return router, request
}

// addRelayRecoveryChannel 创建明确优先级的本地 mock 渠道，验证仍沿用同模型原生分发。
func addRelayRecoveryChannel(t *testing.T, db *gorm.DB, id int, modelName, baseURL string, priority int64) {
	t.Helper()
	channel := &model.Channel{
		Id: id, Type: constant.ChannelTypeOpenAI, Name: "Independent test channel",
		Status: common.ChannelStatusEnabled, Key: "test-only-key", Models: modelName,
		Group: "default", BaseURL: &baseURL, Priority: &priority, AutoBan: common.GetPointer(0),
	}
	require.NoError(t, db.Create(channel).Error)
	require.NoError(t, channel.AddAbilities(db))
}

// relayRecoveryTransport 以正式 HTTP 客户端接口模拟上游，便于确定性验证一分钟截止。
type relayRecoveryTransport func(*http.Request) (*http.Response, error)

// RoundTrip 保留请求上下文和模型传递，只有网络边界使用本地模拟。
func (transport relayRecoveryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

// TestRelayRecoveryCapsPersistentFailures 验证正式分发循环持续 503 时只发出五次同模型请求。
func TestRelayRecoveryCapsPersistentFailures(t *testing.T) {
	if runRelayRecoveryInIsolatedProcess(t) {
		return
	}
	synctest.Test(t, func(t *testing.T) {
		const modelName = "custom-model"
		db := newRelayRecoveryFixture(t, modelName)
		addRelayRecoveryChannel(t, db, 1, modelName, "http://provider.test", 0)
		requests := 0
		service.GetHttpClient().Transport = relayRecoveryTransport(func(request *http.Request) (*http.Response, error) {
			requests++
			var body struct {
				Model string `json:"model"`
			}
			require.NoError(t, common.DecodeJson(request.Body, &body))
			assert.Equal(t, modelName, body.Model)
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"message":"temporarily unavailable"}`)),
				Request:    request,
			}, nil
		})
		router, request := newRelayRecoveryRequest(t, modelName, false)
		recorder := httptest.NewRecorder()
		start := time.Now()
		router.ServeHTTP(recorder, request)
		assert.Equal(t, http.StatusServiceUnavailable, recorder.Code)
		assert.Equal(t, 5, requests)
		assert.GreaterOrEqual(t, time.Since(start), 14500*time.Millisecond)
		assert.LessOrEqual(t, time.Since(start), 29*time.Second)
	})
}

// TestRelayRecoveryCapsCrossGroupRetries 验证原生跨组重置重试索引不能绕过总五次上限。
func TestRelayRecoveryCapsCrossGroupRetries(t *testing.T) {
	if runRelayRecoveryInIsolatedProcess(t) {
		return
	}
	synctest.Test(t, func(t *testing.T) {
		const modelName = "custom-model"
		db := newRelayRecoveryFixture(t, modelName)
		for id, group := range []string{"recovery-a", "recovery-b", "recovery-c"} {
			channel := &model.Channel{
				Id: id + 1, Type: constant.ChannelTypeOpenAI, Name: "Independent test channel",
				Status: common.ChannelStatusEnabled, Key: "test-only-key", Models: modelName,
				Group: group, BaseURL: common.GetPointer("http://provider.test"), AutoBan: common.GetPointer(0),
			}
			require.NoError(t, db.Create(channel).Error)
			require.NoError(t, channel.AddAbilities(db))
		}
		previousAutoGroups := setting.AutoGroups2JsonString()
		previousUsableGroups := setting.UserUsableGroups2JSONString()
		previousGroupRatios := ratio_setting.GroupRatio2JSONString()
		require.NoError(t, setting.UpdateAutoGroupsByJsonString(`["recovery-a","recovery-b","recovery-c"]`))
		require.NoError(t, setting.UpdateUserUsableGroupsByJSONString(`{"default":"Default","recovery-a":"A","recovery-b":"B","recovery-c":"C"}`))
		require.NoError(t, ratio_setting.UpdateGroupRatioByJSONString(`{"default":1,"recovery-a":1,"recovery-b":1,"recovery-c":1}`))
		t.Cleanup(func() {
			require.NoError(t, setting.UpdateAutoGroupsByJsonString(previousAutoGroups))
			require.NoError(t, setting.UpdateUserUsableGroupsByJSONString(previousUsableGroups))
			require.NoError(t, ratio_setting.UpdateGroupRatioByJSONString(previousGroupRatios))
		})
		requests := 0
		service.GetHttpClient().Transport = relayRecoveryTransport(func(request *http.Request) (*http.Response, error) {
			requests++
			_, err := io.Copy(io.Discard, request.Body)
			require.NoError(t, err)
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"message":"temporarily unavailable"}`)),
				Request:    request,
			}, nil
		})
		// 在请求进入正式分发前开启原生自动分组与跨组重试，不改生产分组配置。
		router, request := newRelayRecoveryRequest(t, modelName, false, func(c *gin.Context) {
			common.SetContextKey(c, constant.ContextKeyUsingGroup, "auto")
			common.SetContextKey(c, constant.ContextKeyTokenGroup, "auto")
			common.SetContextKey(c, constant.ContextKeyTokenCrossGroupRetry, true)
			c.Next()
			assert.Equal(t, []string{"1", "1", "1", "2", "2"}, c.GetStringSlice("use_channel"))
		})
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		assert.Equal(t, http.StatusServiceUnavailable, recorder.Code)
		assert.Equal(t, 5, requests)
	})
}

// TestRelayRecoveryCancelsSlowRetry 验证一分钟预算贯穿正式 Relay 和共同 HTTP 出口。
func TestRelayRecoveryCancelsSlowRetry(t *testing.T) {
	if runRelayRecoveryInIsolatedProcess(t) {
		return
	}
	synctest.Test(t, func(t *testing.T) {
		const modelName = "custom-model"
		db := newRelayRecoveryFixture(t, modelName)
		addRelayRecoveryChannel(t, db, 1, modelName, "http://provider.test", 0)
		requests := 0
		service.GetHttpClient().Transport = relayRecoveryTransport(func(request *http.Request) (*http.Response, error) {
			requests++
			_, err := io.Copy(io.Discard, request.Body)
			require.NoError(t, err)
			if requests == 1 {
				return &http.Response{
					StatusCode: http.StatusServiceUnavailable,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(`{"message":"temporarily unavailable"}`)),
					Request:    request,
				}, nil
			}
			<-request.Context().Done()
			return nil, request.Context().Err()
		})
		router, request := newRelayRecoveryRequest(t, modelName, false)
		recorder := httptest.NewRecorder()
		start := time.Now()
		router.ServeHTTP(recorder, request)
		assert.Equal(t, time.Minute, time.Since(start))
		assert.Equal(t, 2, requests)
		assert.Equal(t, http.StatusServiceUnavailable, recorder.Code)
		assert.Contains(t, recorder.Body.String(), "upstream recovery timed out after 60 seconds; model unchanged")
	})
}

// TestRelayRecoveryPreservesEarlierClientDeadline 验证客户端先超时不会误报恢复窗口已耗尽。
func TestRelayRecoveryPreservesEarlierClientDeadline(t *testing.T) {
	if runRelayRecoveryInIsolatedProcess(t) {
		return
	}
	synctest.Test(t, func(t *testing.T) {
		const modelName = "custom-model"
		db := newRelayRecoveryFixture(t, modelName)
		addRelayRecoveryChannel(t, db, 1, modelName, "http://provider.test", 0)
		requests := 0
		service.GetHttpClient().Transport = relayRecoveryTransport(func(request *http.Request) (*http.Response, error) {
			requests++
			_, err := io.Copy(io.Discard, request.Body)
			require.NoError(t, err)
			if requests == 1 {
				return &http.Response{
					StatusCode: http.StatusServiceUnavailable,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(`{"message":"temporarily unavailable"}`)),
					Request:    request,
				}, nil
			}
			<-request.Context().Done()
			return nil, request.Context().Err()
		})
		router, request := newRelayRecoveryRequest(t, modelName, false)
		parent, cancel := context.WithTimeout(request.Context(), 3*time.Second)
		defer cancel()
		request = request.WithContext(parent)
		recorder := httptest.NewRecorder()
		start := time.Now()
		router.ServeHTTP(recorder, request)
		assert.Equal(t, 3*time.Second, time.Since(start))
		assert.Equal(t, 2, requests)
		assert.NotContains(t, recorder.Body.String(), "upstream recovery timed out after 60 seconds")
	})
}

// TestRelayRecoveryFailsOverWithoutChangingModel 验证非 CPA 503 切到独立备用渠道，且上游请求始终保持模型。
func TestRelayRecoveryFailsOverWithoutChangingModel(t *testing.T) {
	if runRelayRecoveryInIsolatedProcess(t) {
		return
	}
	for _, modelName := range []string{"gpt-6.1-sol", "gpt-6-astra", "custom-model"} {
		t.Run(modelName, func(t *testing.T) {
			db := newRelayRecoveryFixture(t, modelName)
			var primaryRequests, backupRequests atomic.Int32
			seenModels := make(chan string, 2)
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				primaryRequests.Add(1)
				var body struct {
					Model string `json:"model"`
				}
				_ = common.DecodeJson(r.Body, &body)
				seenModels <- body.Model
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, `{"error":{"message":"provider maintenance","type":"server_error","code":"server_error"}}`)
			}))
			defer primary.Close()
			backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				backupRequests.Add(1)
				var body struct {
					Model string `json:"model"`
				}
				_ = common.DecodeJson(r.Body, &body)
				seenModels <- body.Model
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"id":"resp_recovered","object":"response","status":"completed","output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}`)
			}))
			defer backup.Close()
			addRelayRecoveryChannel(t, db, 1, modelName, primary.URL, 10)
			addRelayRecoveryChannel(t, db, 2, modelName, backup.URL, 0)
			router, request := newRelayRecoveryRequest(t, modelName, false)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			assert.Equal(t, http.StatusOK, recorder.Code)
			assert.Contains(t, recorder.Body.String(), "resp_recovered")
			assert.EqualValues(t, 1, primaryRequests.Load())
			assert.EqualValues(t, 1, backupRequests.Load())
			assert.Equal(t, modelName, <-seenModels)
			assert.Equal(t, modelName, <-seenModels)
		})
	}
}

// TestRelayRecoveryReturnsRetryAfterBeyondBudget 验证超长上游等待不占满一分钟或提前重试。
func TestRelayRecoveryReturnsRetryAfterBeyondBudget(t *testing.T) {
	if runRelayRecoveryInIsolatedProcess(t) {
		return
	}
	const modelName = "custom-model"
	db := newRelayRecoveryFixture(t, modelName)
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"message":"provider maintenance"}`)
	}))
	defer upstream.Close()
	addRelayRecoveryChannel(t, db, 1, modelName, upstream.URL, 0)
	router, request := newRelayRecoveryRequest(t, modelName, false)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	assert.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	assert.Equal(t, "120", recorder.Header().Get("Retry-After"))
	assert.EqualValues(t, 1, requests.Load())
}

// TestRelayRecoveryStopsAfterStreamCommit 验证已恢复但断流时保留已有 SSE，不重放或混入 JSON。
func TestRelayRecoveryStopsAfterStreamCommit(t *testing.T) {
	if runRelayRecoveryInIsolatedProcess(t) {
		return
	}
	const modelName = "custom-model"
	db := newRelayRecoveryFixture(t, modelName)
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"message":"temporarily unavailable"}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\ndata: invalid-json\n\n")
		w.(http.Flusher).Flush()
	}))
	defer upstream.Close()
	addRelayRecoveryChannel(t, db, 1, modelName, upstream.URL, 0)
	router, request := newRelayRecoveryRequest(t, modelName, true)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	assert.EqualValues(t, 2, requests.Load())
	assert.Contains(t, recorder.Body.String(), "partial")
	assert.NotContains(t, recorder.Body.String(), `{"error":`)
}

// TestRelayRecoveryNoChannelFailsImmediately 验证手动停用等零候选情况不进入上游或恢复等待。
func TestRelayRecoveryNoChannelFailsImmediately(t *testing.T) {
	if runRelayRecoveryInIsolatedProcess(t) {
		return
	}
	const modelName = "custom-model"
	_ = newRelayRecoveryFixture(t, modelName)
	router, request := newRelayRecoveryRequest(t, modelName, false)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	assert.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	assert.Contains(t, recorder.Body.String(), "No available channel")
}
