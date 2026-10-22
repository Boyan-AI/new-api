package controller

import (
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/service"
	"github.com/gin-gonic/gin"
)

// channelRecovery 对所有模型和渠道共用有界恢复，不依赖渠道重试索引避免跨组重置绕过上限。
type channelRecovery struct {
	attempts int
	deadline time.Time
	delay    time.Duration
	cleanup  func()
}

// beginAttempt 保留第一次请求，后续尝试同时尊重总次数、取消、已输出和恢复截止。
func (r *channelRecovery) beginAttempt(c *gin.Context) bool {
	if r.attempts >= 5 {
		return false
	}
	if r.attempts > 0 && (c.Writer.Written() || c.Request.Context().Err() != nil ||
		(!r.deadline.IsZero() && !time.Now().Before(r.deadline))) {
		return false
	}
	r.attempts++
	return true
}

// isTransientUpstreamFailure 只为临时上游故障增加等待，永久请求、渠道或模型配置错误沿用原规则。
func isTransientUpstreamFailure(upstreamErr *types.NewAPIError) bool {
	if upstreamErr == nil || types.IsChannelError(upstreamErr) || types.IsSkipRetryError(upstreamErr) {
		return false
	}
	switch upstreamErr.GetErrorCode() {
	case types.ErrorCodeGetChannelFailed, types.ErrorCodeModelNotFound, types.ErrorCodeInvalidRequest,
		types.ErrorCodeInvalidApiType, types.ErrorCodeConvertRequestFailed:
		return false
	}
	code := upstreamErr.StatusCode
	return code == http.StatusRequestTimeout || code == http.StatusTooManyRequests || code >= 500 && code <= 599
}

// wait 对每次临时失败退避并尊重上游提示；总预算覆盖后续请求，且耗尽后不会重建窗口。
func (r *channelRecovery) wait(c *gin.Context, upstreamErr *types.NewAPIError) bool {
	if common.RetryTimes <= 0 || r.attempts >= 5 || !isTransientUpstreamFailure(upstreamErr) || !shouldRetry(c, upstreamErr, 1) {
		return false
	}
	if r.deadline.IsZero() {
		r.deadline = time.Now().Add(time.Minute)
		r.delay = 2 * time.Second
		r.cleanup = service.ApplyUpstreamRecoveryBudget(c, r.deadline)
	}
	remaining := time.Until(r.deadline)
	if remaining <= 0 {
		return false
	}
	// 等幅抖动避免并发请求同步重试；有效的上游提示始终是等待下限。
	delay := r.delay/2 + time.Duration(rand.Int64N(int64(r.delay/2)+1))
	delay = max(delay, service.UpstreamRetryDelay(upstreamErr))
	if delay >= remaining {
		return false
	}
	logger.LogInfo(c, "Transient upstream failure; waiting before retrying the same model")
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-c.Request.Context().Done():
		return false
	case <-timer.C:
	}
	r.delay = min(r.delay*2, 15*time.Second)
	return time.Now().Before(r.deadline) && c.Request.Context().Err() == nil && !c.Writer.Written()
}

// close 结束恢复计时并还原请求上下文和写入器，不影响原有计费及中间件生命周期。
func (r *channelRecovery) close() {
	if r.cleanup != nil {
		r.cleanup()
	}
}
