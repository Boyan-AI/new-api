package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUpstreamRecoveryBudgetCancelsUncommittedRequest 验证未输出的上游等待不能越过恢复截止。
func TestUpstreamRecoveryBudgetCancelsUncommittedRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		parent := c.Request.Context()
		writer := c.Writer
		cleanup := ApplyUpstreamRecoveryBudget(c, time.Now().Add(time.Minute))
		defer cleanup()
		ctx := c.Request.Context()
		// 设置响应状态或 SSE 头不代表已经向客户端输出。
		c.Writer.WriteHeader(http.StatusOK)
		c.Writer.Header().Set("Content-Type", "text/event-stream")
		start := time.Now()
		<-ctx.Done()
		assert.Equal(t, time.Minute, time.Since(start))
		assert.ErrorIs(t, context.Cause(ctx), context.DeadlineExceeded)
		assert.NoError(t, parent.Err(), "recovery must not cancel the caller's parent context")
		cleanup()
		assert.Same(t, writer, c.Writer)
		assert.Equal(t, parent, c.Request.Context())
	})
}

// TestUpstreamRecoveryBudgetReleasesOnCommit 验证所有响应提交入口解除预算，但客户端取消继续生效。
func TestUpstreamRecoveryBudgetReleasesOnCommit(t *testing.T) {
	tests := []struct {
		name   string
		commit func(*gin.Context)
	}{
		{"write", func(c *gin.Context) { _, _ = c.Writer.Write([]byte("data: started\n\n")) }},
		{"write string", func(c *gin.Context) { _, _ = c.Writer.WriteString("data: started\n\n") }},
		{"flush", func(c *gin.Context) { c.Writer.Flush() }},
		{"write header now", func(c *gin.Context) { c.Writer.WriteHeaderNow() }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(parent)
				cleanup := ApplyUpstreamRecoveryBudget(c, time.Now().Add(time.Minute))
				defer cleanup()
				ctx := c.Request.Context()
				tt.commit(c)
				require.True(t, c.Writer.Written())
				time.Sleep(2 * time.Minute)
				assert.NoError(t, ctx.Err(), "a recovered long response must not be truncated at 60 seconds")
				cancel()
				assert.ErrorIs(t, ctx.Err(), context.Canceled)
			})
		})
	}
}

// TestUpstreamRecoveryBudgetPreservesEarlierCancellation 验证客户端原有截止优先于一分钟恢复窗口。
func TestUpstreamRecoveryBudgetPreservesEarlierCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		parent, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(parent)
		cleanup := ApplyUpstreamRecoveryBudget(c, time.Now().Add(time.Minute))
		defer cleanup()
		start := time.Now()
		<-c.Request.Context().Done()
		assert.Equal(t, 3*time.Second, time.Since(start))
		assert.ErrorIs(t, context.Cause(c.Request.Context()), context.DeadlineExceeded)
	})
}
