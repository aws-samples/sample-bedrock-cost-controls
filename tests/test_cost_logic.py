"""
Unit tests for the cost-calculation, cache-pricing and policy-scoping logic.

Pure-function tests: no AWS calls, no deployed stack required.

    python -m pytest tests/ -v
    python tests/test_cost_logic.py      # also runs standalone

Covers the defects found during design review:

1. Prompt cache tokens were not metered at all, because `input.inputTokenCount`
   in the invocation log envelope EXCLUDES cache read/write tokens.
2. The IAM deny policy was unscoped, so on a shared role one user exceeding
   budget would have blocked Bedrock for every user of that role.
3. Cache multipliers were global, but the real ratios differ by provider family:
   Anthropic charges 1.25x input for cache writes while Amazon Nova charges $0.00.
4. Cache writes are billed by TTL. Pricing every write at the 5-minute rate
   underprices 1-hour writes by 37.5%.
5. Current-generation Claude pricing lives under a different Price List service
   code (`AmazonBedrockFoundationModels`), quoted per 1M tokens rather than 1K.
"""

import importlib.util
import json
import os
import sys
from decimal import Decimal

os.environ.setdefault("PRICING_TABLE", "t")
os.environ.setdefault("USAGE_TABLE", "t")
os.environ.setdefault("BUDGET_CONFIG_TABLE", "t")
os.environ.setdefault("DEDUP_TABLE", "t")
os.environ.setdefault("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789012:t")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

_ROOT = os.path.join(os.path.dirname(__file__), "..")


def _load(module_name: str, relative_path: str):
    """
    Load a handler by path under a distinct module name.

    Both Lambdas are named `handler.py`, so a plain `import handler` would resolve
    to whichever directory happens to come first on sys.path.
    """
    spec = importlib.util.spec_from_file_location(
        module_name, os.path.join(_ROOT, relative_path)
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


enf = _load("enforcement_handler", "lambdas/cost_enforcement/handler.py")
sync = _load("pricing_sync_handler", "lambdas/pricing_sync/handler.py")


# Claude Sonnet 4.5 Global rates, per 1K tokens, as published by
# AmazonBedrockFoundationModels in us-east-1.
SONNET_GLOBAL = {
    "input_price_per_1k_tokens": Decimal("0.003"),
    "output_price_per_1k_tokens": Decimal("0.015"),
    "cache_read_price_per_1k_tokens": Decimal("0.0003"),
    "cache_write_price_per_1k_tokens": Decimal("0.00375"),
    "cache_write_1h_price_per_1k_tokens": Decimal("0.006"),
}
# Same model, no cache columns — forces the multiplier fallback.
SONNET_NO_CACHE = {
    "input_price_per_1k_tokens": Decimal("0.003"),
    "output_price_per_1k_tokens": Decimal("0.015"),
}
NOVA_PRO_NO_CACHE = {
    "input_price_per_1k_tokens": Decimal("0.0008"),
    "output_price_per_1k_tokens": Decimal("0.0032"),
}

CLAUDE = "anthropic.claude-sonnet-4-5-20250929-v1:0"
CLAUDE_CRIS = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
NOVA = "amazon.nova-pro-v1:0"


def _usage(inp=0, out=0, cache_read=0, cache_write=0, w5m=None, w1h=0):
    return {
        "input": inp, "output": out, "cache_read": cache_read,
        "cache_write": cache_write,
        "cache_write_5m": cache_write if w5m is None else w5m,
        "cache_write_1h": w1h,
    }


# ---------------------------------------------------------------------------
# Token extraction
# ---------------------------------------------------------------------------


def _record(usage=None, envelope_in=0, envelope_out=0):
    rec = {
        "requestId": "r1",
        "modelId": CLAUDE,
        "identity": {"arn": "arn:aws:sts::123456789012:assumed-role/SharedRole/alice"},
        "input": {"inputTokenCount": envelope_in},
        "output": {"outputTokenCount": envelope_out},
    }
    if usage is not None:
        rec["output"]["outputBodyJson"] = {"usage": usage}
    return rec


def test_anthropic_native_cache_extracted():
    u = enf._extract_token_usage(_record(
        usage={"input_tokens": 350, "output_tokens": 900,
               "cache_read_input_tokens": 120_000,
               "cache_creation_input_tokens": 0},
        envelope_in=350, envelope_out=900,
    ))
    assert u["source"] == "anthropic_native"
    assert (u["input"], u["cache_read"], u["cache_write"]) == (350, 120_000, 0)


def test_converse_cache_extracted():
    u = enf._extract_token_usage(_record(usage={
        "inputTokens": 500, "outputTokens": 250,
        "cacheReadInputTokens": 10_000, "cacheWriteInputTokens": 2_000,
    }))
    assert u["source"] == "converse"
    assert (u["input"], u["cache_read"], u["cache_write"]) == (500, 10_000, 2_000)


def test_openai_convention_is_inverted_and_not_double_counted():
    """OpenAI reports input_tokens INCLUDING cache, so the remainder is derived."""
    u = enf._extract_token_usage(_record(usage={
        "input_tokens": 3671, "output_tokens": 100,
        "input_tokens_details": {"cached_tokens": 3626, "cache_write_tokens": 0},
    }))
    assert u["source"] == "openai"
    assert u["input"] == 45, "cached prefix must not be counted twice"
    assert u["cache_read"] == 3626


def test_envelope_only_is_flagged():
    u = enf._extract_token_usage(_record(envelope_in=2000, envelope_out=900))
    assert u["source"] == "envelope_only"
    assert (u["input"], u["cache_read"]) == (2000, 0)


def test_usage_without_cache_fields():
    u = enf._extract_token_usage(_record(usage={"inputTokens": 100, "outputTokens": 50}))
    assert u["source"] == "usage_no_cache"


def test_missing_and_malformed_counts_do_not_raise():
    assert enf._extract_token_usage({})["input"] == 0
    u = enf._extract_token_usage(_record(usage={
        "inputTokens": None, "outputTokens": "abc",
        "cacheReadInputTokens": Decimal("5"),
    }))
    assert (u["input"], u["output"], u["cache_read"]) == (0, 0, 5)


# ---------------------------------------------------------------------------
# Cache-write TTL split
# ---------------------------------------------------------------------------


def test_converse_cache_details_splits_by_ttl():
    rec = _record(usage={
        "inputTokens": 100, "outputTokens": 50,
        "cacheReadInputTokens": 0, "cacheWriteInputTokens": 30_000,
        "cacheDetails": [{"inputTokens": 20_000, "ttl": "1h"},
                         {"inputTokens": 10_000, "ttl": "5m"}],
    })
    assert enf._split_cache_writes(rec, 30_000) == (10_000, 20_000)


def test_anthropic_cache_creation_splits_by_ttl():
    rec = _record(usage={
        "input_tokens": 100, "output_tokens": 50,
        "cache_read_input_tokens": 0, "cache_creation_input_tokens": 30_000,
        "cache_creation": {"ephemeral_5m_input_tokens": 10_000,
                           "ephemeral_1h_input_tokens": 20_000},
    })
    assert enf._split_cache_writes(rec, 30_000) == (10_000, 20_000)


def test_writes_default_to_5m_when_no_breakdown():
    """Documented default: absent an explicit ttl, caching is 5-minute."""
    rec = _record(usage={"inputTokens": 100, "outputTokens": 50,
                         "cacheWriteInputTokens": 5_000, "cacheReadInputTokens": 0})
    assert enf._split_cache_writes(rec, 5_000) == (5_000, 0)


def test_no_cache_writes_splits_to_zero():
    assert enf._split_cache_writes(_record(), 0) == (0, 0)


def test_malformed_cache_details_does_not_raise():
    rec = _record(usage={"cacheWriteInputTokens": 100, "cacheReadInputTokens": 0,
                         "cacheDetails": ["not-a-dict", {"ttl": "1h"}]})
    w5m, w1h = enf._split_cache_writes(rec, 100)
    assert w5m + w1h >= 0


# ---------------------------------------------------------------------------
# Model family
# ---------------------------------------------------------------------------


def test_model_family_ignores_cross_region_prefix():
    assert enf._model_family(CLAUDE) == "anthropic"
    assert enf._model_family(CLAUDE_CRIS) == "anthropic"
    assert enf._model_family("eu.anthropic.claude-opus-4-5") == "anthropic"
    assert enf._model_family("apac.anthropic.claude-haiku-4-5") == "anthropic"
    assert enf._model_family(NOVA) == "amazon"
    assert enf._model_family("meta.llama3-3-70b-instruct-v1:0") == "meta"
    assert enf._model_family("") == ""


# ---------------------------------------------------------------------------
# Cost calculation
# ---------------------------------------------------------------------------


def test_no_cache_activity_matches_legacy_two_term_result():
    """Regression guard: uncached traffic must price exactly as it always did."""
    cost, _bd, est = enf._calculate_cost(_usage(inp=2000, out=900), SONNET_GLOBAL, CLAUDE)
    assert cost == Decimal("0.0195")
    assert est == [], "nothing should be flagged as estimated"


def test_published_cache_rates_are_used_verbatim():
    cost, bd, est = enf._calculate_cost(
        _usage(inp=350, out=900, cache_read=120_000), SONNET_GLOBAL, CLAUDE)
    assert bd["cache_read"] == Decimal("36.000") / 1000
    assert cost == Decimal("0.05055")
    assert est == []


def test_1h_cache_writes_cost_more_than_5m():
    """1h writes are 2.00x input vs 1.25x for 5m — a 60% difference."""
    w5m, _, _ = enf._calculate_cost(
        _usage(cache_write=120_000, w5m=120_000), SONNET_GLOBAL, CLAUDE)
    w1h, _, _ = enf._calculate_cost(
        _usage(cache_write=120_000, w5m=0, w1h=120_000), SONNET_GLOBAL, CLAUDE)
    assert w5m == Decimal("0.45000")
    assert w1h == Decimal("0.72000")
    assert w1h > w5m
    # Pricing a 1h write at the 5m rate would understate it by 37.5%
    assert (w1h - w5m) / w1h == Decimal("0.375")


def test_mixed_ttl_writes_priced_separately():
    cost, bd, _ = enf._calculate_cost(
        _usage(cache_write=30_000, w5m=10_000, w1h=20_000), SONNET_GLOBAL, CLAUDE)
    assert bd["cache_write_5m"] == (Decimal(10_000) / 1000) * Decimal("0.00375")
    assert bd["cache_write_1h"] == (Decimal(20_000) / 1000) * Decimal("0.006")
    assert cost == bd["cache_write_5m"] + bd["cache_write_1h"]


def test_anthropic_multiplier_fallback_matches_published_ratios():
    """
    With no cache columns, the derived Anthropic rates must equal the real ones.
    This is what makes the fallback exact rather than approximate.
    """
    u = _usage(cache_read=1000, cache_write=1000, w5m=1000)
    fallback, fb, est = enf._calculate_cost(u, SONNET_NO_CACHE, CLAUDE)
    published, pb, _ = enf._calculate_cost(u, SONNET_GLOBAL, CLAUDE)
    assert fb["cache_read"] == pb["cache_read"]
    assert fb["cache_write_5m"] == pb["cache_write_5m"]
    assert fallback == published
    assert set(est) == {"cache_read", "cache_write"}


def test_nova_multiplier_fallback_does_not_invent_write_charges():
    """Amazon Nova cache writes are free and reads are 0.25x, not 0.10x/1.25x."""
    u = _usage(cache_read=1000, cache_write=1000, w5m=1000)
    _cost, bd, _est = enf._calculate_cost(u, NOVA_PRO_NO_CACHE, NOVA)
    p_in = NOVA_PRO_NO_CACHE["input_price_per_1k_tokens"]
    assert bd["cache_write_5m"] == 0, "Nova does not charge for cache writes"
    assert bd["cache_read"] == (Decimal(1000) / 1000) * p_in * Decimal("0.25")
    # The Anthropic ratios would have produced a non-zero write charge
    anthropic_write = (Decimal(1000) / 1000) * p_in * Decimal("1.25")
    assert bd["cache_write_5m"] != anthropic_write


def test_unknown_family_uses_conservative_default():
    u = _usage(cache_read=1000, cache_write=1000, w5m=1000)
    _cost, bd, _ = enf._calculate_cost(u, SONNET_NO_CACHE, "someprovider.some-model-v1")
    p_in = SONNET_NO_CACHE["input_price_per_1k_tokens"]
    assert bd["cache_write_5m"] == (Decimal(1000) / 1000) * p_in * Decimal("1.25")


def test_estimated_only_reported_when_component_has_tokens():
    """A model with no cache columns but no cache traffic is not an estimate."""
    _cost, _bd, est = enf._calculate_cost(_usage(inp=100, out=100), SONNET_NO_CACHE, CLAUDE)
    assert est == []


def test_cache_tokens_never_price_at_zero_for_anthropic():
    _cost, bd, _ = enf._calculate_cost(
        _usage(cache_read=1000, cache_write=1000, w5m=1000), SONNET_NO_CACHE, CLAUDE)
    assert bd["cache_read"] > 0 and bd["cache_write_5m"] > 0


def test_zero_token_model_prices_at_zero():
    """Image/video models report no tokens. Documented coverage gap."""
    cost, _bd, _est = enf._calculate_cost(_usage(), SONNET_GLOBAL, "amazon.nova-canvas-v1:0")
    assert cost == Decimal("0")


# ---------------------------------------------------------------------------
# Deny policy scoping
# ---------------------------------------------------------------------------


def test_role_deny_is_scoped_to_the_session():
    enf._role_id_cache["SharedRole"] = "AROAEXAMPLEID123"
    doc = json.loads(enf._build_deny_policy("alice", "role", "SharedRole"))
    stmt = doc["Statement"][0]
    assert stmt["Effect"] == "Deny"
    assert stmt["Condition"]["StringLike"]["aws:userid"] == "AROAEXAMPLEID123:alice"


def test_role_deny_refused_when_it_cannot_be_scoped():
    original_flag = enf.REQUIRE_SCOPED_DENY
    original_fn = enf._get_role_id
    enf.REQUIRE_SCOPED_DENY = True
    enf._get_role_id = lambda name: None
    try:
        assert enf._build_deny_policy("bob", "role", "UnknownRole") is None
    finally:
        enf.REQUIRE_SCOPED_DENY = original_flag
        enf._get_role_id = original_fn


def test_user_deny_needs_no_condition():
    doc = json.loads(enf._build_deny_policy("carol", "user", ""))
    assert "Condition" not in doc["Statement"][0]


def test_policy_stays_within_iam_inline_size_limits():
    enf._role_id_cache["R"] = "AROAEXAMPLEID123"
    doc = enf._build_deny_policy("dave", "role", "R")
    size = len(json.dumps(json.loads(doc), separators=(",", ":")))
    assert size < 2048 // 4, f"policy unexpectedly large: {size} chars"


def test_principal_parsing():
    role_arn = "arn:aws:sts::123456789012:assumed-role/SharedRole/alice"
    assert enf._extract_user_id(role_arn) == "alice"
    assert enf._extract_principal_info(role_arn)["role_name"] == "SharedRole"
    user_arn = "arn:aws:iam::123456789012:user/bob"
    assert enf._extract_principal_info(user_arn)["principal_type"] == "user"
    assert enf._extract_user_id("") == ""


# ---------------------------------------------------------------------------
# Pricing sync — unit normalization
# ---------------------------------------------------------------------------


def test_per_1m_prices_normalize_to_per_1k():
    """
    AmazonBedrockFoundationModels quotes per 1M tokens; AmazonBedrock per 1K.
    Mixing them without normalizing would misprice by 1000x.
    """
    assert sync._normalize_to_per_1k(Decimal("3.30"), "1M tokens") == Decimal("0.0033")
    assert sync._normalize_to_per_1k(Decimal("0.003"), "1K tokens") == Decimal("0.003")
    # Unknown unit is assumed per-1K rather than silently divided
    assert sync._normalize_to_per_1k(Decimal("5"), "tokens") == Decimal("5")


def test_servicename_normalizes_to_model_name():
    assert sync._normalize_model_name(
        "Claude Sonnet 4.5 (Amazon Bedrock Edition)") == "claudesonnet45"
    assert sync._normalize_model_name("Claude Sonnet 4.5") == "claudesonnet45"
    assert sync._normalize_model_name(
        "Claude 3.5 Sonnet v2 (Amazon Bedrock Edition)") == "claude35sonnetv2"
    assert sync._normalize_model_name("") == ""


def test_fm_usagetype_regex_identifies_component_and_scope():
    cases = [
        ("USE1-MP:USE1_InputTokenCount-Units", "input", "regional", False),
        ("USE1-MP:USE1_InputTokenCount_Global-Units", "input", "global", False),
        ("USE1-MP:USE1_OutputTokenCount_Global-Units", "output", "global", False),
        ("USE1-MP:USE1_CacheReadInputTokenCount-Units", "cache_read", "regional", False),
        ("USE1-MP:USE1_CacheWriteInputTokenCount_Global-Units", "cache_write", "global", False),
        ("USE1-MP:USE1_CacheWrite1hInputTokenCount-Units", "cache_write_1h", "regional", False),
        ("USE1-MP:USE1_InputTokenCount_Batch-Units", "input", "regional", True),
    ]
    for usagetype, want_component, want_scope, want_batch in cases:
        m = sync._FM_USAGETYPE_RE.search(usagetype)
        assert m, f"did not match: {usagetype}"
        assert sync._FM_COMPONENTS[m.group("component")] == want_component, usagetype
        assert ("global" if m.group("scope") else "regional") == want_scope, usagetype
        assert bool(m.group("batch")) == want_batch, usagetype

    # Reserved / provisioned TPM rates must not match
    for skip in ("USE1-MP:USE1_Reserved_1Month_InputTPM_Geo-Units",
                 "USE1-MP:USE1_Reserved_3Month_OutputTPM_Global-Units"):
        assert sync._FM_USAGETYPE_RE.search(skip) is None, skip


def _fm_record(usagetype, servicename, usd, unit="1M tokens"):
    return {
        "product": {"attributes": {"usagetype": usagetype, "servicename": servicename}},
        "terms": {"OnDemand": {"t": {"priceDimensions": {"d": {
            "pricePerUnit": {"USD": usd}, "unit": unit}}}}},
    }


def test_fm_price_map_captures_both_scopes_and_normalizes():
    name = "Claude Sonnet 4.5 (Amazon Bedrock Edition)"
    records = [
        _fm_record("USE1-MP:USE1_InputTokenCount-Units", name, "3.3000000000"),
        _fm_record("USE1-MP:USE1_OutputTokenCount-Units", name, "16.5000000000"),
        _fm_record("USE1-MP:USE1_CacheReadInputTokenCount-Units", name, "0.3300000000"),
        _fm_record("USE1-MP:USE1_CacheWriteInputTokenCount-Units", name, "4.1250000000"),
        _fm_record("USE1-MP:USE1_CacheWrite1hInputTokenCount-Units", name, "6.6000000000"),
        _fm_record("USE1-MP:USE1_InputTokenCount_Global-Units", name, "3.0000000000"),
        _fm_record("USE1-MP:USE1_OutputTokenCount_Global-Units", name, "15.0000000000"),
        _fm_record("USE1-MP:USE1_CacheReadInputTokenCount_Global-Units", name, "0.3000000000"),
        _fm_record("USE1-MP:USE1_CacheWriteInputTokenCount_Global-Units", name, "3.7500000000"),
        _fm_record("USE1-MP:USE1_CacheWrite1hInputTokenCount_Global-Units", name, "6.0000000000"),
        # Batch must be excluded
        _fm_record("USE1-MP:USE1_InputTokenCount_Batch-Units", name, "1.6500000000"),
    ]
    pm = sync._build_fm_price_map(records)
    entry = pm["claudesonnet45"]

    assert entry["regional"]["input"] == Decimal("0.0033")
    assert entry["regional"]["cache_read"] == Decimal("0.00033")
    assert entry["regional"]["cache_write"] == Decimal("0.004125")
    assert entry["regional"]["cache_write_1h"] == Decimal("0.0066")

    assert entry["global"]["input"] == Decimal("0.003")
    assert entry["global"]["cache_read"] == Decimal("0.0003")
    assert entry["global"]["cache_write"] == Decimal("0.00375")
    assert entry["global"]["cache_write_1h"] == Decimal("0.006")

    # Verified published ratios: 0.10x read, 1.25x 5m write, 2.00x 1h write
    for scope in ("regional", "global"):
        p = entry[scope]
        assert p["cache_read"] / p["input"] == Decimal("0.10")
        assert p["cache_write"] / p["input"] == Decimal("1.25")
        assert p["cache_write_1h"] / p["input"] == Decimal("2.00")


def test_fm_match_prefers_configured_rate_scope():
    pm = {"claudesonnet45": {
        "regional": {"input": Decimal("0.0033"), "output": Decimal("0.0165")},
        "global": {"input": Decimal("0.003"), "output": Decimal("0.015")},
    }}
    original = sync.RATE_SCOPE
    try:
        sync.RATE_SCOPE = "global"
        prices, scope = sync._match_fm_pricing({"modelName": "Claude Sonnet 4.5"}, pm)
        assert scope == "global" and prices["input"] == Decimal("0.003")

        sync.RATE_SCOPE = "regional"
        prices, scope = sync._match_fm_pricing({"modelName": "Claude Sonnet 4.5"}, pm)
        assert scope == "regional" and prices["input"] == Decimal("0.0033")
    finally:
        sync.RATE_SCOPE = original


def test_fm_match_falls_back_when_preferred_scope_missing():
    pm = {"claudeopus45": {"regional": {"input": Decimal("0.0055"),
                                        "output": Decimal("0.0275")}}}
    original = sync.RATE_SCOPE
    try:
        sync.RATE_SCOPE = "global"
        prices, scope = sync._match_fm_pricing({"modelName": "Claude Opus 4.5"}, pm)
        assert scope == "regional" and prices is not None
    finally:
        sync.RATE_SCOPE = original


def test_fm_match_returns_none_for_unknown_model():
    prices, scope = sync._match_fm_pricing({"modelName": "Totally Unknown 9"}, {})
    assert prices is None and scope == ""


def test_usable_rejects_zero_input_but_allows_zero_output_and_cache():
    """
    A $0.00 input rate signals a placeholder SKU and must not be accepted, or all
    cost for that model silently becomes zero. Zero output (embeddings) and zero
    cache write (Nova) are legitimate.
    """
    assert not sync._usable({"input": Decimal("0"), "output": Decimal("1")})
    assert sync._usable({"input": Decimal("0.003"), "output": Decimal("0")})
    assert sync._usable({"input": Decimal("0.0008"), "output": Decimal("0.0032"),
                         "cache_write": Decimal("0")})
    assert not sync._usable({"input": Decimal("0.003")})


def test_legacy_usagetype_key_strips_cache_suffixes():
    assert sync._usagetype_to_model_key("NovaPro-cache-read-input-token-count") == "NovaPro"
    assert sync._usagetype_to_model_key("NovaPro-cache-write-input-token-count") == "NovaPro"
    assert sync._usagetype_to_model_key("NovaLite-input-tokens") == "NovaLite"
    assert sync._usagetype_to_model_key("Claude3Haiku-output-tokens") == "Claude3Haiku"


def test_zero_price_is_returned_not_skipped():
    """Nova publishes $0.00 for cache writes; treating it as missing is wrong."""
    price, unit = sync._extract_price_and_unit(
        {"t": {"priceDimensions": {"d": {"pricePerUnit": {"USD": "0.0000000000"},
                                         "unit": "1K tokens"}}}})
    assert price == Decimal("0")
    assert unit == "1K tokens"


if __name__ == "__main__":
    tests = [(n, o) for n, o in sorted(globals().items())
             if n.startswith("test_") and callable(o)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"  PASS  {name}")
        except AssertionError as e:
            failed += 1
            print(f"  FAIL  {name}: {e}")
        except Exception as e:
            failed += 1
            print(f"  ERROR {name}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
