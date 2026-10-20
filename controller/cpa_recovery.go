package controller

import (
	"net/http"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/gin-gonic/gin"
)

// cpaRecovery 为单次请求保留独立恢复预算，避免瞬时失败耗尽所有重试。
type cpaRecovery struct {
	deadline time.Time
	delay    time.Duration
}

// wait 仅等待托管本地 CPA 的明确无认证错误；取消、已输出响应或超时均立即停止恢复。
func (r *cpaRecovery) wait(c *gin.Context, channel *model.Channel, upstreamErr *types.NewAPIError) bool {
	if common.RetryTimes <= 0 || !shouldRetry(c, upstreamErr, 1) || upstreamErr.StatusCode != http.StatusServiceUnavailable {
		return false
	}
	if c.Request.Method != http.MethodPost || c.Request.URL.Path != "/v1/responses" ||
		channel == nil || channel.Name != "Codex Auth CPA" || channel.GetBaseURL() != "http://127.0.0.1:8317" ||
		!strings.HasPrefix(upstreamErr.Error(), "auth_unavailable: no auth available") {
		return false
	}
	if r.deadline.IsZero() {
		r.deadline = time.Now().Add(time.Minute)
		r.delay = 2 * time.Second
	}
	remaining := time.Until(r.deadline)
	if remaining <= 0 {
		return false
	}
	delay := min(r.delay, remaining)
	logger.LogInfo(c, "CPA authentication temporarily unavailable; waiting before retrying")
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
