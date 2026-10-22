package service

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/gin-gonic/gin"
)

// upstreamRetryHint 保留上游重试时刻，不把响应头或内部状态写入客户端错误正文。
type upstreamRetryHint struct {
	err     error
	retryAt time.Time
}

// Error 保持原有错误消息，避免重试元数据改变公开错误契约。
func (e *upstreamRetryHint) Error() string { return e.err.Error() }

// Unwrap 允许错误解析和状态转换继续访问原始错误。
func (e *upstreamRetryHint) Unwrap() error { return e.err }

// parseUpstreamRetryAfter 支持秒数和 HTTP 日期，并饱和转换以避免恶意大数溢出。
func parseUpstreamRetryAfter(value string, receivedAt time.Time) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false
	}
	if seconds, err := strconv.ParseUint(value, 10, 64); err == nil || errors.Is(err, strconv.ErrRange) {
		seconds = min(seconds, uint64(math.MaxInt64/int64(time.Second)))
		return receivedAt.Add(time.Duration(seconds) * time.Second), true
	}
	retryAt, err := http.ParseTime(value)
	return retryAt, err == nil
}

// UpstreamRetryDelay 返回仍需等待的时间；无效或已过期的提示不延迟请求。
func UpstreamRetryDelay(err *types.NewAPIError) time.Duration {
	var hint *upstreamRetryHint
	if !errors.As(err, &hint) {
		return 0
	}
	return max(time.Until(hint.retryAt), 0)
}

// recoveryResponseWriter 在真正提交响应时释放恢复预算，正常长流仍继承客户端取消。
type recoveryResponseWriter struct {
	gin.ResponseWriter
	release func()
}

// WriteHeaderNow 覆盖 Gin 的显式响应提交入口，不把仅设置状态码视为已输出。
func (w *recoveryResponseWriter) WriteHeaderNow() {
	w.release()
	w.ResponseWriter.WriteHeaderNow()
}

// Write 在首次内容输出时解除恢复截止，避免已恢复的回答被一分钟上限截断。
func (w *recoveryResponseWriter) Write(data []byte) (int, error) {
	w.release()
	return w.ResponseWriter.Write(data)
}

// WriteString 保持字符串输出与普通字节输出相同的恢复截止语义。
func (w *recoveryResponseWriter) WriteString(data string) (int, error) {
	w.release()
	return w.ResponseWriter.WriteString(data)
}

// Flush 视为响应已提交，之后禁止重放或以恢复预算取消正常流。
func (w *recoveryResponseWriter) Flush() {
	w.release()
	w.ResponseWriter.Flush()
}

// Unwrap 让标准库响应控制器继续访问原有写入截止等能力。
func (w *recoveryResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// ApplyUpstreamRecoveryBudget 将等待和未输出的上游请求统一纳入截止，返回幂等清理函数。
func ApplyUpstreamRecoveryBudget(c *gin.Context, deadline time.Time) func() {
	parent := c.Request.Context()
	writer := c.Writer
	ctx, cancel := context.WithCancelCause(parent)
	var releaseOnce sync.Once
	timer := time.AfterFunc(time.Until(deadline), func() {
		releaseOnce.Do(func() { cancel(context.DeadlineExceeded) })
	})
	release := func() { releaseOnce.Do(func() { timer.Stop() }) }
	c.Request = c.Request.WithContext(ctx)
	c.Writer = &recoveryResponseWriter{ResponseWriter: writer, release: release}
	var cleanupOnce sync.Once
	return func() {
		cleanupOnce.Do(func() {
			release()
			cancel(nil)
			c.Request = c.Request.WithContext(parent)
			c.Writer = writer
		})
	}
}
