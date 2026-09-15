package controller

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/pkg/billingexpr"
	"github.com/QuantumNous/new-api/relay"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/QuantumNous/new-api/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateChannelProxy(t *testing.T) {
	tests := []struct {
		name    string
		proxy   string
		wantErr bool
	}{
		{name: "empty"},
		{name: "http", proxy: "http://proxy.example:8080"},
		{name: "https", proxy: "https://proxy.example:8443"},
		{name: "socks5", proxy: "socks5://proxy.example"},
		{name: "socks5h", proxy: "socks5h://proxy.example:1080/"},
		{name: "unsupported", proxy: "ftp://proxy.example", wantErr: true},
		{name: "path", proxy: "socks5://proxy.example:1080/path", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setting, err := common.Marshal(dto.ChannelSettings{Proxy: test.proxy})
			require.NoError(t, err)
			channel := &model.Channel{
				Type:    constant.ChannelTypeOpenAI,
				Setting: common.GetPointer(string(setting)),
			}

			err = validateChannel(channel, false)

			if test.wantErr {
				require.ErrorContains(t, err, "invalid channel proxy")
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestChannelForSingleKeyTestBindsRequestedKey(t *testing.T) {
	original := &model.Channel{
		Id:  7,
		Key: "first-key\nsecond-key",
		ChannelInfo: model.ChannelInfo{
			IsMultiKey:   true,
			MultiKeySize: 2,
		},
	}

	selected, err := channelForSingleKeyTest(original, 1)

	require.NoError(t, err)
	assert.Equal(t, "second-key", selected.Key)
	assert.False(t, selected.ChannelInfo.IsMultiKey)
	assert.Equal(t, 0, selected.ChannelInfo.MultiKeySize)
	assert.True(t, original.ChannelInfo.IsMultiKey)
}

func TestChannelForSingleKeyTestRejectsInvalidIndex(t *testing.T) {
	channel := &model.Channel{
		Key:         "only-key",
		ChannelInfo: model.ChannelInfo{IsMultiKey: true, MultiKeySize: 1},
	}

	_, err := channelForSingleKeyTest(channel, 1)

	require.ErrorContains(t, err, "out of range")
}

func TestMultiKeyAliasIDIsStableAndDoesNotExposeKey(t *testing.T) {
	first := multiKeyAliasID("sk-secret-value")
	second := multiKeyAliasID("sk-secret-value")

	assert.Equal(t, first, second)
	assert.Len(t, first, 64)
	assert.NotContains(t, first, "secret")
}

func TestMultiKeyAliasForDisplayMasksKeyByDefault(t *testing.T) {
	assert.Equal(t, "***", multiKeyAliasForDisplay("abc", nil))
	assert.Equal(t, "sk-1**********cdef", multiKeyAliasForDisplay("sk-1234567890abcdef", nil))
}

func TestMultiKeyAliasForDisplayPrefersCustomAlias(t *testing.T) {
	key := "sk-1234567890abcdef"
	aliases := map[string]string{multiKeyAliasID(key): "production"}

	assert.Equal(t, "production", multiKeyAliasForDisplay(key, aliases))
}

func TestMultiKeyAliasForDisplayFallsBackForBlankAlias(t *testing.T) {
	key := "sk-1234567890abcdef"
	aliases := map[string]string{multiKeyAliasID(key): "  "}

	assert.Equal(t, "sk-1**********cdef", multiKeyAliasForDisplay(key, aliases))
}

func TestPruneMultiKeyAliasesKeepsAliasesByKeyFingerprint(t *testing.T) {
	keys := []string{"key-b", "key-a"}
	aliases := map[string]string{
		multiKeyAliasID("key-a"):   "A",
		multiKeyAliasID("key-b"):   "B",
		multiKeyAliasID("removed"): "stale",
	}

	pruneMultiKeyAliases(keys, aliases)

	assert.Equal(t, map[string]string{
		multiKeyAliasID("key-a"): "A",
		multiKeyAliasID("key-b"): "B",
	}, aliases)
}

func TestNormalizeChannelTestEndpointTreatsAutoAsAutomatic(t *testing.T) {
	channel := &model.Channel{Type: constant.ChannelTypeOpenAI}

	assert.Empty(t, normalizeChannelTestEndpoint(channel, "auto"))
	assert.Empty(t, normalizeChannelTestEndpoint(channel, " AUTO "))
}

func TestValidateChannelRequiresNewAPIBaseURL(t *testing.T) {
	tests := []struct {
		name    string
		baseURL *string
		wantErr bool
	}{
		{name: "missing", wantErr: true},
		{name: "blank", baseURL: common.GetPointer("  "), wantErr: true},
		{name: "configured", baseURL: common.GetPointer("https://new-api.example")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			channel := &model.Channel{
				Type:    constant.ChannelTypeNewAPI,
				BaseURL: test.baseURL,
			}

			err := validateChannel(channel, false)

			if test.wantErr {
				require.ErrorContains(t, err, "New API channel base URL cannot be empty")
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestNewAPIChannelRegistration(t *testing.T) {
	apiType, ok := common.ChannelType2APIType(constant.ChannelTypeNewAPI)

	require.True(t, ok)
	assert.Equal(t, constant.APITypeNewAPI, apiType)
	assert.Equal(t, "New API", constant.GetChannelTypeName(constant.ChannelTypeNewAPI))
	require.Greater(t, len(constant.ChannelBaseURLs), constant.ChannelTypeNewAPI)
	assert.Empty(t, constant.ChannelBaseURLs[constant.ChannelTypeNewAPI])
}

func TestResponsesCompactChannelSupport(t *testing.T) {
	tests := []struct {
		name        string
		channelType int
		apiType     int
		want        bool
	}{
		{name: "OpenAI", channelType: constant.ChannelTypeOpenAI, apiType: constant.APITypeOpenAI, want: true},
		{name: "Azure", channelType: constant.ChannelTypeAzure, apiType: constant.APITypeOpenAI, want: true},
		{name: "Codex", channelType: constant.ChannelTypeCodex, apiType: constant.APITypeCodex, want: true},
		{name: "Advanced Custom", channelType: constant.ChannelTypeAdvancedCustom, apiType: constant.APITypeAdvancedCustom, want: true},
		{name: "Sub2API", channelType: constant.ChannelTypeSub2API, apiType: constant.APITypeSub2API, want: true},
		{name: "New API", channelType: constant.ChannelTypeNewAPI, apiType: constant.APITypeNewAPI, want: true},
		{name: "Anthropic", channelType: constant.ChannelTypeAnthropic, apiType: constant.APITypeAnthropic, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, common.SupportsResponsesCompact(test.channelType, test.apiType))
		})
	}
}

// TestBuildTestRequestResponsesIncludesCodexCompatibilityFields 验证显式 Responses 测试复现 Codex CLI 的稳定请求字段。
func TestBuildTestRequestResponsesIncludesCodexCompatibilityFields(t *testing.T) {
	request, ok := buildTestRequest("gpt-5.6-sol", string(constant.EndpointTypeOpenAIResponse), &model.Channel{Type: constant.ChannelTypeOpenAI}, true).(*dto.OpenAIResponsesRequest)
	require.True(t, ok)
	assert.Empty(t, request.Instructions)
	assert.JSONEq(t, `[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]`, string(request.Input))
	assert.JSONEq(t, `["reasoning.encrypted_content"]`, string(request.Include))
	assert.JSONEq(t, `false`, string(request.ParallelToolCalls))
	assert.JSONEq(t, `"`+codexChannelTestID+`"`, string(request.PromptCacheKey))
	require.NotNil(t, request.Reasoning)
	assert.Equal(t, "low", request.Reasoning.Effort)
	assert.JSONEq(t, `"all_turns"`, string(request.Reasoning.Context))
	assert.JSONEq(t, `false`, string(request.Store))
	assert.JSONEq(t, `{"verbosity":"low"}`, string(request.Text))
	assert.JSONEq(t, `"auto"`, string(request.ToolChoice))
	assert.JSONEq(t, `{
		"turn_id":"00000000-0000-4000-8000-000000000000",
		"session_id":"00000000-0000-4000-8000-000000000000",
		"thread_id":"00000000-0000-4000-8000-000000000000",
		"root_turn_id":"00000000-0000-4000-8000-000000000000",
		"x-codex-installation-id":"00000000-0000-4000-8000-000000000000"
	}`, string(request.ClientMetadata))
	assert.Equal(t, "gpt-5.6-sol", request.Model)
	assert.True(t, *request.Stream)
}

// TestBuildTestRequestCodexIncludesCodexCompatibilityFields 验证自动识别的 Codex 模型复用同一真实请求契约。
func TestBuildTestRequestCodexIncludesCodexCompatibilityFields(t *testing.T) {
	request, ok := buildTestRequest("gpt-5.6-codex", "", &model.Channel{Type: constant.ChannelTypeOpenAI}, false).(*dto.OpenAIResponsesRequest)
	require.True(t, ok)
	assert.Empty(t, request.Instructions)
	assert.JSONEq(t, `false`, string(request.Store))
	require.NotNil(t, request.Stream)
	assert.True(t, *request.Stream)
}

// TestAnyRouterResponsesChannelTestSendsCodexCLIContract 验证最终出站请求满足 AnyRouter 用于识别 Codex 客户端的协议契约。
func TestAnyRouterResponsesChannelTestSendsCodexCLIContract(t *testing.T) {
	type capturedRequest struct {
		header http.Header
		body   []byte
		err    error
	}
	captured := make(chan capturedRequest, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		captured <- capturedRequest{header: request.Header.Clone(), body: body, err: err}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{}`))
	}))
	defer upstream.Close()

	gin.SetMode(gin.TestMode)
	contextRecorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(contextRecorder)
	context.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	context.Request.Header.Set("Content-Type", "application/json")

	request := buildCodexResponsesTestRequest("gpt-6-astra")
	info := relaycommon.GenRelayInfoResponses(context, request)
	info.IsChannelTest = true
	info.RequestURLPath = "/v1/responses"
	info.ChannelMeta = &relaycommon.ChannelMeta{
		ChannelType:       constant.ChannelTypeOpenAI,
		ChannelBaseUrl:    upstream.URL,
		ApiKey:            "test-key",
		UpstreamModelName: "gpt-6-astra",
		HeadersOverride: map[string]interface{}{
			"anthropic-beta": "context-1m-2025-08-07",
		},
	}
	configureCodexResponsesChannelTest(context, info)

	adaptor := relay.GetAdaptor(constant.APITypeOpenAI)
	require.NotNil(t, adaptor)
	adaptor.Init(info)
	converted, err := adaptor.ConvertOpenAIResponsesRequest(context, info, *request)
	require.NoError(t, err)
	body, err := common.Marshal(converted)
	require.NoError(t, err)

	response, err := adaptor.DoRequest(context, info, bytes.NewReader(body))
	require.NoError(t, err)
	require.NotNil(t, response)
	require.NoError(t, response.(*http.Response).Body.Close())

	actual := <-captured
	require.NoError(t, actual.err)
	assert.Equal(t, "Bearer test-key", actual.header.Get("Authorization"))
	assert.Equal(t, "text/event-stream", actual.header.Get("Accept"))
	assert.Equal(t, codexChannelTestOriginator, actual.header.Get("Originator"))
	assert.Equal(t, codexChannelTestUserAgent, actual.header.Get("User-Agent"))
	assert.Equal(t, "context-1m-2025-08-07", actual.header.Get("Anthropic-Beta"))
	assert.JSONEq(t, `{
		"model":"gpt-6-astra",
		"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}],
		"include":["reasoning.encrypted_content"],
		"parallel_tool_calls":false,
		"prompt_cache_key":"00000000-0000-4000-8000-000000000000",
		"reasoning":{"effort":"low","context":"all_turns"},
		"store":false,
		"stream":true,
		"text":{"verbosity":"low"},
		"tool_choice":"auto",
		"client_metadata":{
			"turn_id":"00000000-0000-4000-8000-000000000000",
			"session_id":"00000000-0000-4000-8000-000000000000",
			"thread_id":"00000000-0000-4000-8000-000000000000",
			"root_turn_id":"00000000-0000-4000-8000-000000000000",
			"x-codex-installation-id":"00000000-0000-4000-8000-000000000000"
		}
	}`, string(actual.body))
}

func TestMultiprotocolGatewayEndpointTypes(t *testing.T) {
	want := []constant.EndpointType{
		constant.EndpointTypeOpenAI,
		constant.EndpointTypeOpenAIResponse,
		constant.EndpointTypeOpenAIResponseCompact,
		constant.EndpointTypeAnthropic,
		constant.EndpointTypeGemini,
		constant.EndpointTypeOpenAIAlphaSearch,
	}

	assert.Equal(t, want, common.GetEndpointTypesByChannelType(constant.ChannelTypeNewAPI, "gpt-5"))
	assert.Equal(t, want, common.GetEndpointTypesByChannelType(constant.ChannelTypeSub2API, "gpt-5"))
}

func TestCopyChannelRejectsInvalidLegacyProxySettings(t *testing.T) {
	db := setupModelListControllerTestDB(t)
	settingBytes, err := common.Marshal(dto.ChannelSettings{
		Proxy: "socks5://proxy.example/legacy-path",
	})
	require.NoError(t, err)
	setting := string(settingBytes)
	origin := &model.Channel{
		Type:    constant.ChannelTypeOpenAI,
		Name:    "legacy proxy channel",
		Key:     "test-key",
		Models:  "gpt-test",
		Group:   "default",
		Setting: &setting,
	}
	require.NoError(t, db.Create(origin).Error)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "id", Value: fmt.Sprintf("%d", origin.Id)}}
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/channel/copy", nil)

	CopyChannel(ctx)

	assert.Contains(t, recorder.Body.String(), "invalid channel settings")
	var channelCount int64
	require.NoError(t, db.Model(&model.Channel{}).Count(&channelCount).Error)
	assert.Equal(t, int64(1), channelCount)
}

func TestDeleteChannelResetsProxyCacheWhenPreReadFails(t *testing.T) {
	db := setupModelListControllerTestDB(t)
	require.NoError(t, db.AutoMigrate(&model.Log{}))
	service.ResetProxyClientCache()
	t.Cleanup(service.ResetProxyClientCache)

	proxyURL := "http://proxy.example:8080"
	beforeDelete, err := service.GetHttpClientWithProxy(proxyURL)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "id", Value: "999999"}}
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/api/channel/999999", nil)

	DeleteChannel(ctx)

	assert.Contains(t, recorder.Body.String(), `"success":true`)
	afterDelete, err := service.GetHttpClientWithProxy(proxyURL)
	require.NoError(t, err)
	assert.NotSame(t, beforeDelete, afterDelete)
}

func TestDeleteChannelBatchReportsAndAuditsActualDeletedCount(t *testing.T) {
	db := setupModelListControllerTestDB(t)
	require.NoError(t, db.AutoMigrate(&model.Log{}))
	channel := &model.Channel{Name: "existing", Key: "test-key"}
	require.NoError(t, db.Create(channel).Error)

	requestBody, err := common.Marshal(ChannelBatch{Ids: []int{channel.Id, 999999}})
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/api/channel/batch", bytes.NewReader(requestBody))
	ctx.Request.Header.Set("Content-Type", "application/json")

	DeleteChannelBatch(ctx)

	var response struct {
		Success bool  `json:"success"`
		Data    int64 `json:"data"`
	}
	require.NoError(t, common.Unmarshal(recorder.Body.Bytes(), &response))
	assert.True(t, response.Success)
	assert.Equal(t, int64(1), response.Data)

	var auditLog model.Log
	require.NoError(t, db.Order("id desc").First(&auditLog).Error)
	var auditData struct {
		Operation struct {
			Params map[string]any `json:"params"`
		} `json:"op"`
	}
	require.NoError(t, common.UnmarshalJsonStr(auditLog.Other, &auditData))
	assert.Equal(t, float64(1), auditData.Operation.Params["count"])
}

func TestSettleTestQuotaUsesTieredBilling(t *testing.T) {
	info := &relaycommon.RelayInfo{
		TieredBillingSnapshot: &billingexpr.BillingSnapshot{
			BillingMode:   "tiered_expr",
			ExprString:    `param("stream") == true ? tier("stream", p * 3) : tier("base", p * 2)`,
			ExprHash:      billingexpr.ExprHashString(`param("stream") == true ? tier("stream", p * 3) : tier("base", p * 2)`),
			GroupRatio:    1,
			EstimatedTier: "stream",
			QuotaPerUnit:  common.QuotaPerUnit,
			ExprVersion:   1,
		},
		BillingRequestInput: &billingexpr.RequestInput{
			Body: []byte(`{"stream":true}`),
		},
	}

	quota, result := settleTestQuota(info, types.PriceData{
		ModelRatio:      1,
		CompletionRatio: 2,
	}, &dto.Usage{
		PromptTokens: 1000,
	})

	require.Equal(t, 1500, quota)
	require.NotNil(t, result)
	require.Equal(t, "stream", result.MatchedTier)
}

func TestBuildTestLogOtherInjectsTieredInfo(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())

	info := &relaycommon.RelayInfo{
		TieredBillingSnapshot: &billingexpr.BillingSnapshot{
			BillingMode: "tiered_expr",
			ExprString:  `tier("base", p * 2)`,
		},
		ChannelMeta: &relaycommon.ChannelMeta{},
	}
	priceData := types.PriceData{
		GroupRatioInfo: types.GroupRatioInfo{GroupRatio: 1},
	}
	usage := &dto.Usage{
		PromptTokensDetails: dto.InputTokenDetails{
			CachedTokens: 12,
		},
	}

	requestRules := []billingexpr.RequestRuleTrace{{
		Cond:       `param("service_tier") == "fast"`,
		Multiplier: 2,
		Matched:    true,
	}}
	other := buildTestLogOther(ctx, info, priceData, usage, &billingexpr.TieredResult{
		MatchedTier:  "base",
		RequestRules: requestRules,
	})

	require.Equal(t, "tiered_expr", other["billing_mode"])
	require.Equal(t, "base", other["matched_tier"])
	require.Equal(t, requestRules, other["request_rules"])
	require.NotEmpty(t, other["expr_b64"])
}

func TestResolveChannelTestUserIDUsesRequestUser(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Set("id", 2)

	userID, err := resolveChannelTestUserID(ctx)

	require.NoError(t, err)
	require.Equal(t, 2, userID)
}

func TestSelectChannelsForAutomaticTestPassiveRecoveryOnlyUsesAutoDisabled(t *testing.T) {
	channels := []*model.Channel{
		{Id: 1, Status: common.ChannelStatusEnabled},
		{Id: 2, Status: common.ChannelStatusAutoDisabled},
		{Id: 3, Status: common.ChannelStatusManuallyDisabled},
	}

	selected := selectChannelsForAutomaticTest(channels, operation_setting.ChannelTestModePassiveRecovery)

	require.Len(t, selected, 1)
	require.Equal(t, 2, selected[0].Id)
}

func TestSelectChannelsForAutomaticTestScheduledSkipsManualDisabled(t *testing.T) {
	channels := []*model.Channel{
		{Id: 1, Status: common.ChannelStatusEnabled},
		{Id: 2, Status: common.ChannelStatusAutoDisabled},
		{Id: 3, Status: common.ChannelStatusManuallyDisabled},
	}

	selected := selectChannelsForAutomaticTest(channels, operation_setting.ChannelTestModeScheduledAll)

	require.Len(t, selected, 2)
	require.Equal(t, 1, selected[0].Id)
	require.Equal(t, 2, selected[1].Id)
}

func TestSelectChannelsForAutomaticTestAutoBanOnlyUsesEligibleChannels(t *testing.T) {
	autoBanEnabled := 1
	autoBanDisabled := 0
	channels := []*model.Channel{
		{Id: 1, Status: common.ChannelStatusEnabled, AutoBan: &autoBanEnabled},
		{Id: 2, Status: common.ChannelStatusEnabled, AutoBan: &autoBanDisabled},
		{Id: 3, Status: common.ChannelStatusAutoDisabled, AutoBan: &autoBanEnabled},
		{Id: 4, Status: common.ChannelStatusManuallyDisabled, AutoBan: &autoBanEnabled},
		{Id: 5, Status: common.ChannelStatusEnabled},
	}

	selected := selectChannelsForAutomaticTest(channels, operation_setting.ChannelTestModeAutoBanOnly)

	require.Len(t, selected, 2)
	require.Equal(t, 1, selected[0].Id)
	require.Equal(t, 3, selected[1].Id)
}

func TestRunChannelTestWorkersHonorsConfiguredConcurrency(t *testing.T) {
	originalInterval := common.RequestInterval
	common.RequestInterval = 0
	t.Cleanup(func() { common.RequestInterval = originalInterval })

	channels := []*model.Channel{
		{Id: 1, Status: common.ChannelStatusEnabled},
		{Id: 2, Status: common.ChannelStatusEnabled},
		{Id: 3, Status: common.ChannelStatusEnabled},
		{Id: 4, Status: common.ChannelStatusEnabled},
	}
	started := make(chan struct{}, len(channels))
	release := make(chan struct{})
	var active atomic.Int32
	var maxActive atomic.Int32
	progress := make([]int, 0, len(channels)+1)
	summaryResult := make(chan channelTestSummary, 1)

	go func() {
		summaryResult <- runChannelTestWorkers(
			context.Background(),
			channels,
			2,
			func(_ context.Context, _ *model.Channel) channelTestSummary {
				current := active.Add(1)
				defer active.Add(-1)
				for {
					observed := maxActive.Load()
					if current <= observed || maxActive.CompareAndSwap(observed, current) {
						break
					}
				}
				started <- struct{}{}
				<-release
				return channelTestSummary{Tested: 1, Succeeded: 1}
			},
			func(processed, _ int) {
				progress = append(progress, processed)
			},
		)
	}()

	<-started
	<-started
	select {
	case <-started:
		t.Fatal("started more channel tests than the configured concurrency")
	default:
	}
	close(release)

	summary := <-summaryResult

	assert.Equal(t, int32(2), maxActive.Load())
	assert.Equal(t, channelTestSummary{Tested: 4, Succeeded: 4}, summary)
	assert.Equal(t, []int{0, 1, 2, 3, 4}, progress)
}

func TestRunChannelTestWorkersStopsAfterCancellation(t *testing.T) {
	originalInterval := common.RequestInterval
	common.RequestInterval = 0
	t.Cleanup(func() { common.RequestInterval = originalInterval })

	ctx, cancel := context.WithCancel(context.Background())
	channels := []*model.Channel{
		{Id: 1, Status: common.ChannelStatusEnabled},
		{Id: 2, Status: common.ChannelStatusEnabled},
		{Id: 3, Status: common.ChannelStatusEnabled},
		{Id: 4, Status: common.ChannelStatusEnabled},
	}
	started := make(chan struct{}, len(channels))
	progress := make([]int, 0, 1)
	summaryResult := make(chan channelTestSummary, 1)

	go func() {
		summaryResult <- runChannelTestWorkers(
			ctx,
			channels,
			2,
			func(ctx context.Context, _ *model.Channel) channelTestSummary {
				started <- struct{}{}
				<-ctx.Done()
				return channelTestSummary{Tested: 1, Succeeded: 1}
			},
			func(processed, _ int) {
				progress = append(progress, processed)
			},
		)
	}()

	<-started
	<-started
	cancel()

	summary := <-summaryResult

	select {
	case <-started:
		t.Fatal("started another channel test after cancellation")
	default:
	}
	assert.Equal(t, channelTestSummary{Tested: 2, Succeeded: 2}, summary)
	assert.Equal(t, []int{0}, progress)
}

func TestTestAllChannelsRejectsExistingActiveTask(t *testing.T) {
	db := setupModelListControllerTestDB(t)
	require.NoError(t, db.AutoMigrate(&model.SystemTask{}, &model.SystemTaskLock{}))

	existing, err := model.CreateSystemTask(model.SystemTaskTypeChannelTest, nil, nil)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/channel/test", nil)

	TestAllChannels(ctx)

	require.Equal(t, http.StatusConflict, recorder.Code)
	require.Contains(t, recorder.Body.String(), existing.TaskID)
	require.Contains(t, recorder.Body.String(), "已有通道测试任务正在运行或等待中")
}
