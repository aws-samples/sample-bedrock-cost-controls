"""
Cost Enforcement Lambda — triggered by CloudWatch Logs Subscription Filter
on the Bedrock Model Invocation Log group.

Responsibilities:
1. Decode and decompress CloudWatch Logs payload
2. For each invocation record, deduplicate, calculate cost (including prompt
   cache read/write tokens), atomically increment user spend in DynamoDB, and
   enforce budget limits via a session-scoped IAM deny policy.

IMPORTANT — prompt caching
--------------------------
The `input.inputTokenCount` field in the invocation log envelope EXCLUDES tokens
read from or written to the prompt cache. Per the Bedrock documentation:

    total input tokens = inputTokens + cacheReadInputTokens + cacheWriteInputTokens

Metering on the envelope alone therefore undercounts any cached workload, and
undercounts agentic coding assistants (Claude Code, Kiro, Cline) severely, because
cached context dominates their token volume. A cold-cache request can be
undercounted by more than an order of magnitude.

The cache counts are recoverable from `output.outputBodyJson.usage`, which this
handler parses. See `_extract_token_usage` for the per-provider shapes and note
that OpenAI-family models invert the convention (their `input_tokens` INCLUDES
cache tokens).

Cache writes are additionally billed by TTL. For Anthropic the 5-minute tier is
1.25x the input rate while the 1-hour tier is 2.00x, so `_split_cache_writes`
separates them and `_calculate_cost` prices five components rather than four.

IMPORTANT — shared roles
------------------------
`user_id` is derived from the *session name* in `identity.arn`, but IAM inline
policies attach to the *role*. On a shared role (the normal IAM Identity Center
pattern, where many people assume one role) an unscoped deny would block Bedrock
for every user of that role. The deny policy is therefore scoped with a condition
on `aws:userid` so it applies only to the offending session. See
`_build_deny_policy`.
"""

import base64
import gzip
import json
import logging
import os
import time
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Clients ---
dynamodb = boto3.resource("dynamodb")
iam_client = boto3.client("iam")
sns_client = boto3.client("sns")

# --- Environment ---
PRICING_TABLE = os.environ["PRICING_TABLE"]
USAGE_TABLE = os.environ["USAGE_TABLE"]
BUDGET_CONFIG_TABLE = os.environ["BUDGET_CONFIG_TABLE"]
DEDUP_TABLE = os.environ["DEDUP_TABLE"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
DEFAULT_DAILY_LIMIT = Decimal(os.environ.get("DEFAULT_DAILY_LIMIT", "50"))
DEFAULT_MONTHLY_LIMIT = Decimal(os.environ.get("DEFAULT_MONTHLY_LIMIT", "500"))

# Fallback cache pricing multipliers, relative to the uncached input rate.
# Used only when the pricing table row has no explicit cache prices, i.e. before
# the Pricing Sync Lambda has populated them from the Price List API.
#
# These ratios are NOT universal across providers, so they are keyed by model
# family. Verified against published Price List rates (us-east-1):
#
#   Anthropic — cache read 0.10x input, 5m write 1.25x, 1h write 2.00x.
#     Confirmed identical across Haiku 4.5, Sonnet 4.5 and Opus 4.5, and across
#     both Regional and Global rate scopes.
#   Amazon Nova — cache read 0.25x input, cache write $0.00 (writes are free).
#     Applying the Anthropic ratios here would invent charges that do not exist.
#
# The default entry uses the Anthropic ratios because they are the conservative
# choice: they price cache writes at a premium rather than free, so a budget is
# more likely to trip early than late for an unknown family.
CACHE_MULTIPLIERS_BY_FAMILY = {
    "anthropic": {"read": Decimal("0.10"), "write": Decimal("1.25"), "write_1h": Decimal("2.00")},
    "amazon": {"read": Decimal("0.25"), "write": Decimal("0"), "write_1h": Decimal("0")},
    "_default": {"read": Decimal("0.10"), "write": Decimal("1.25"), "write_1h": Decimal("2.00")},
}

# Optional environment overrides applied to the default family only.
_ENV_READ_MULTIPLIER = os.environ.get("CACHE_READ_MULTIPLIER")
_ENV_WRITE_MULTIPLIER = os.environ.get("CACHE_WRITE_MULTIPLIER")
_ENV_WRITE_1H_MULTIPLIER = os.environ.get("CACHE_WRITE_1H_MULTIPLIER")
if _ENV_READ_MULTIPLIER:
    CACHE_MULTIPLIERS_BY_FAMILY["_default"]["read"] = Decimal(_ENV_READ_MULTIPLIER)
if _ENV_WRITE_MULTIPLIER:
    CACHE_MULTIPLIERS_BY_FAMILY["_default"]["write"] = Decimal(_ENV_WRITE_MULTIPLIER)
if _ENV_WRITE_1H_MULTIPLIER:
    CACHE_MULTIPLIERS_BY_FAMILY["_default"]["write_1h"] = Decimal(_ENV_WRITE_1H_MULTIPLIER)

# When true (default), refuse to attach a deny policy that cannot be scoped to a
# single session. Failing to deny one user is far less damaging than denying every
# user of a shared role. Set to "false" only if every role in the account is
# dedicated to a single principal.
REQUIRE_SCOPED_DENY = os.environ.get("REQUIRE_SCOPED_DENY", "true").lower() == "true"

METRIC_NAMESPACE = os.environ.get("METRIC_NAMESPACE", "BedrockCostControls")

# --- Table references ---
pricing_table = dynamodb.Table(PRICING_TABLE)
usage_table = dynamodb.Table(USAGE_TABLE)
budget_config_table = dynamodb.Table(BUDGET_CONFIG_TABLE)
dedup_table = dynamodb.Table(DEDUP_TABLE)

# --- In-memory caches (per Lambda execution environment) ---
_pricing_cache: dict[str, dict] = {}
_role_id_cache: dict[str, str] = {}
# Budget config is the hottest read on the path, so cache it briefly.
# TTL keeps limit changes taking effect within BUDGET_CACHE_TTL_SECONDS.
_budget_cache: dict[str, tuple[float, dict]] = {}
BUDGET_CACHE_TTL_SECONDS = int(os.environ.get("BUDGET_CACHE_TTL_SECONDS", "60"))

# --- Fallback pricing for unknown models ---
# Conservative estimate (Sonnet-tier) so we never skip cost tracking when a new
# model appears before the pricing sync runs.
FALLBACK_PRICING = {
    "input_price_per_1k_tokens": Decimal("0.003"),
    "output_price_per_1k_tokens": Decimal("0.015"),
}

# Models billed per token. Anything priced per image, per second of video, per
# model-copy-minute (Custom Model Import) or per model-unit-hour (Provisioned
# Throughput) reports no token counts and will price at $0.00 here. That is a
# known coverage gap, documented in the README.
_ZERO = Decimal("0")


def lambda_handler(event: dict, context) -> dict:
    """Entry point for CloudWatch Logs subscription filter events."""
    records = _decode_cw_logs_event(event)
    logger.info("Processing %d log record(s)", len(records))

    processed = 0
    skipped = 0
    failed = 0

    for record in records:
        try:
            if _process_invocation(record):
                processed += 1
            else:
                skipped += 1
        except Exception:
            failed += 1
            logger.exception("Error processing record: %s", record)

    # Surface failures as a metric. Sustained non-zero values mean spend is being
    # lost, which silently breaks enforcement.
    if failed:
        _emit_metric("RecordProcessingFailures", failed)

    logger.info("Done. processed=%d skipped=%d failed=%d", processed, skipped, failed)
    return {"processed": processed, "skipped": skipped, "failed": failed}


# ---------------------------------------------------------------------------
# Decoding
# ---------------------------------------------------------------------------


def _decode_cw_logs_event(event: dict) -> list[dict]:
    """Decode base64 + gzip CloudWatch Logs payload into log records."""
    compressed = base64.b64decode(event["awslogs"]["data"])
    payload = json.loads(gzip.decompress(compressed))
    log_events = payload.get("logEvents", [])

    records = []
    for log_event in log_events:
        try:
            record = json.loads(log_event["message"])
            records.append(record)
        except (json.JSONDecodeError, KeyError):
            logger.warning("Skipping unparseable log event: %s", log_event.get("id"))
    return records


# ---------------------------------------------------------------------------
# Token usage extraction (prompt-cache aware)
# ---------------------------------------------------------------------------


def _extract_token_usage(record: dict) -> dict:
    """
    Normalize token usage across the response shapes Bedrock emits.

    Returns a dict with non-cached `input`, `output`, `cache_read`, `cache_write`
    and a `source` marker identifying which shape matched.

    Convention differences that matter:

    * Converse / ConverseStream  — `inputTokens` EXCLUDES cache tokens.
    * Anthropic native InvokeModel (Claude Code's path) — `input_tokens`
      EXCLUDES cache tokens.
    * OpenAI-compatible — `input_tokens` INCLUDES cached and written tokens, so
      the non-cached remainder must be derived by subtraction. Applying the
      Anthropic formula here would double-count the entire cached prefix.

    If no usage object is present we fall back to the envelope counts and mark
    the source `envelope_only`, which means cache activity is invisible for that
    record. That condition is emitted as a metric so it can be alarmed on rather
    than passing silently.
    """
    body = record.get("output", {}).get("outputBodyJson") or {}
    usage = body.get("usage") or {}

    # --- Converse / ConverseStream ---
    if "cacheReadInputTokens" in usage or "cacheWriteInputTokens" in usage:
        return {
            "input": _as_int(usage.get("inputTokens")),
            "output": _as_int(usage.get("outputTokens")),
            "cache_read": _as_int(usage.get("cacheReadInputTokens")),
            "cache_write": _as_int(usage.get("cacheWriteInputTokens")),
            "source": "converse",
        }

    # --- Anthropic native (InvokeModel) — Claude Code's path ---
    if "cache_read_input_tokens" in usage or "cache_creation_input_tokens" in usage:
        return {
            "input": _as_int(usage.get("input_tokens")),
            "output": _as_int(usage.get("output_tokens")),
            "cache_read": _as_int(usage.get("cache_read_input_tokens")),
            "cache_write": _as_int(usage.get("cache_creation_input_tokens")),
            "source": "anthropic_native",
        }

    # --- OpenAI-compatible: input_tokens INCLUDES cache, so subtract ---
    details = usage.get("input_tokens_details") or {}
    if details:
        cached = _as_int(details.get("cached_tokens"))
        written = _as_int(details.get("cache_write_tokens"))
        total_in = _as_int(usage.get("input_tokens"))
        return {
            "input": max(total_in - cached - written, 0),
            "output": _as_int(usage.get("output_tokens")),
            "cache_read": cached,
            "cache_write": written,
            "source": "openai",
        }

    # --- Plain usage object with no cache fields (caching not in use) ---
    if usage:
        return {
            "input": _as_int(usage.get("inputTokens", usage.get("input_tokens"))),
            "output": _as_int(usage.get("outputTokens", usage.get("output_tokens"))),
            "cache_read": 0,
            "cache_write": 0,
            "source": "usage_no_cache",
        }

    # --- Envelope only. Cache activity invisible for this record. ---
    return {
        "input": _as_int(
            record.get("input", {}).get("inputTokenCount")
            or record.get("inputTokenCount")
        ),
        "output": _as_int(
            record.get("output", {}).get("outputTokenCount")
            or record.get("outputTokenCount")
        ),
        "cache_read": 0,
        "cache_write": 0,
        "source": "envelope_only",
    }


def _as_int(value) -> int:
    """Coerce a possibly-missing / possibly-Decimal token count to int."""
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _split_cache_writes(record: dict, total_cache_write: int) -> tuple[int, int]:
    """
    Split cache-write tokens into (5-minute TTL, 1-hour TTL).

    Cache writes are billed by TTL: for Anthropic the 5-minute tier is 1.25x the
    input rate while the 1-hour tier is 2.00x, so treating everything as 5m
    underprices 1-hour writes by 37.5%.

    Two shapes carry the breakdown:

    * Converse — `usage.cacheDetails`, an array of `{"inputTokens": N,
      "ttl": "5m" | "1h"}`.
    * Anthropic native — `usage.cache_creation` with `ephemeral_5m_input_tokens`
      and `ephemeral_1h_input_tokens`.

    When neither is present the documented default applies: absent an explicit
    `ttl` on the cache checkpoint, caching is 5-minute. Only Claude Opus 4.5,
    Sonnet 4.5 and Haiku 4.5 support the 1-hour tier at all.
    """
    if total_cache_write <= 0:
        return 0, 0

    usage = (record.get("output", {}).get("outputBodyJson") or {}).get("usage") or {}

    # Converse: explicit per-TTL breakdown
    details = usage.get("cacheDetails")
    if isinstance(details, list) and details:
        write_1h = 0
        write_5m = 0
        for entry in details:
            if not isinstance(entry, dict):
                continue
            tokens = _as_int(entry.get("inputTokens"))
            ttl = str(entry.get("ttl", "")).strip().lower()
            if ttl == "1h":
                write_1h += tokens
            else:
                write_5m += tokens
        if (write_1h + write_5m) > 0:
            return write_5m, write_1h

    # Anthropic native: cache_creation sub-object
    creation = usage.get("cache_creation")
    if isinstance(creation, dict) and creation:
        write_1h = _as_int(creation.get("ephemeral_1h_input_tokens"))
        write_5m = _as_int(creation.get("ephemeral_5m_input_tokens"))
        if (write_1h + write_5m) > 0:
            return write_5m, write_1h
        if write_1h > 0:
            return max(total_cache_write - write_1h, 0), write_1h

    # No breakdown available — documented default is the 5-minute tier.
    return total_cache_write, 0


def _model_family(model_id: str) -> str:
    """
    Derive the provider family from a model ID, ignoring any cross-region
    inference profile prefix.

    "us.anthropic.claude-sonnet-4-5-..."  -> "anthropic"
    "amazon.nova-pro-v1:0"                -> "amazon"
    """
    if not model_id:
        return ""
    parts = model_id.split(".")
    # Strip a leading cross-region prefix such as us. / eu. / apac. / ap.
    if parts and parts[0].lower() in ("us", "eu", "ap", "apac", "us-gov"):
        parts = parts[1:]
    return parts[0].lower() if parts else ""


# ---------------------------------------------------------------------------
# Cost calculation
# ---------------------------------------------------------------------------


def _calculate_cost(
    usage: dict, pricing: dict, model_id: str = ""
) -> tuple[Decimal, dict, list[str]]:
    """
    Five-component cost model: non-cached input, output, cache reads, 5-minute
    cache writes, and 1-hour cache writes.

    Cache rates come from the pricing table when the Pricing Sync Lambda has
    populated them from the Price List API. Anything absent falls back to the
    per-family multiplier relative to the input rate — correct in ratio for
    Anthropic, and correctly zero-write for Amazon Nova.

    Returns (total_cost, per_component_breakdown, estimated_components) where the
    last element names any component priced from a multiplier rather than a
    published rate, so the caller can surface it as a metric.
    """
    p_in = Decimal(str(pricing.get("input_price_per_1k_tokens", _ZERO)))
    p_out = Decimal(str(pricing.get("output_price_per_1k_tokens", _ZERO)))

    family = _model_family(model_id)
    multipliers = CACHE_MULTIPLIERS_BY_FAMILY.get(
        family, CACHE_MULTIPLIERS_BY_FAMILY["_default"]
    )

    estimated: list[str] = []

    def _rate(column: str, multiplier_key: str, label: str) -> Decimal:
        published = pricing.get(column)
        if published is not None:
            return Decimal(str(published))
        estimated.append(label)
        return p_in * multipliers[multiplier_key]

    p_cache_read = _rate("cache_read_price_per_1k_tokens", "read", "cache_read")
    p_cache_write = _rate("cache_write_price_per_1k_tokens", "write", "cache_write")

    # A model with no published 1-hour rate falls back to the 1h multiplier rather
    # than the 5m rate, so long-TTL writes are not silently underpriced.
    p_cache_write_1h = _rate(
        "cache_write_1h_price_per_1k_tokens", "write_1h", "cache_write_1h"
    )

    breakdown = {
        "input": (Decimal(usage["input"]) / Decimal("1000")) * p_in,
        "output": (Decimal(usage["output"]) / Decimal("1000")) * p_out,
        "cache_read": (Decimal(usage["cache_read"]) / Decimal("1000")) * p_cache_read,
        "cache_write_5m": (
            Decimal(usage.get("cache_write_5m", usage["cache_write"])) / Decimal("1000")
        ) * p_cache_write,
        "cache_write_1h": (
            Decimal(usage.get("cache_write_1h", 0)) / Decimal("1000")
        ) * p_cache_write_1h,
    }

    # Only report an estimate as relevant if the component actually had tokens.
    relevant = [
        label for label in estimated
        if (label == "cache_read" and usage["cache_read"] > 0)
        or (label == "cache_write" and usage.get("cache_write_5m", usage["cache_write"]) > 0)
        or (label == "cache_write_1h" and usage.get("cache_write_1h", 0) > 0)
    ]
    return sum(breakdown.values(), _ZERO), breakdown, relevant


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------


def _process_invocation(record: dict) -> bool:
    """
    Process a single Bedrock invocation log record.
    Returns True if processed, False if skipped (duplicate or unusable).
    """
    request_id = record.get("requestId")
    if not request_id:
        logger.warning("Record missing requestId, skipping")
        return False

    identity_arn = record.get("identity", {}).get("arn", "")
    model_id = record.get("modelId", "")
    timestamp = record.get("timestamp", "")

    if not identity_arn or not model_id:
        logger.warning("Record missing identity.arn or modelId: requestId=%s", request_id)
        return False

    user_id = _extract_user_id(identity_arn)
    if not user_id:
        logger.warning("Could not derive user_id from ARN: %s", identity_arn)
        return False

    # Deduplicate only after the record is known to be usable, so a malformed
    # record does not consume its request_id and mask a later valid retry.
    if not _try_claim_request(request_id):
        logger.info("Duplicate requestId=%s, skipping", request_id)
        return False

    usage = _extract_token_usage(record)

    # Cache writes are billed by TTL, so split them before pricing.
    write_5m, write_1h = _split_cache_writes(record, usage["cache_write"])
    usage["cache_write_5m"] = write_5m
    usage["cache_write_1h"] = write_1h

    pricing = _get_pricing(model_id)
    if not pricing:
        logger.warning(
            "No pricing found for model_id=%s, using fallback pricing", model_id
        )
        pricing = FALLBACK_PRICING
        _emit_metric("FallbackPricingUsed", 1, model_id=model_id)

    invocation_cost, breakdown, estimated = _calculate_cost(usage, pricing, model_id)

    # A caching-capable request metered from the envelope alone is a known
    # undercount. Alarm on this rather than letting it pass silently.
    if usage["source"] == "envelope_only":
        _emit_metric("EnvelopeOnlyMetering", 1, model_id=model_id)

    # Cache priced from a family multiplier rather than a published rate. Not an
    # error, but it means the figure is an estimate — make that observable.
    for component in estimated:
        _emit_metric("EstimatedCachePricing", 1, model_id=model_id, component=component)

    logger.info(
        "user=%s model=%s src=%s in=%d out=%d cache_read=%d cache_write=%d "
        "(5m=%d 1h=%d) cost=$%s estimated=%s",
        user_id, model_id, usage["source"], usage["input"], usage["output"],
        usage["cache_read"], usage["cache_write"], write_5m, write_1h,
        invocation_cost, ",".join(estimated) or "none",
    )

    updated = _increment_usage(user_id, invocation_cost, usage, timestamp, identity_arn)

    _emit_metric("AttributedCostUsd", float(invocation_cost), unit="None",
                 usage_source=usage["source"])
    if usage["cache_read"] or usage["cache_write"]:
        # Enables reconciliation against the Bedrock CacheReadInputTokens /
        # CacheWriteInputTokens runtime metrics.
        _emit_metric("CacheReadTokens", usage["cache_read"])
        _emit_metric("CacheWriteTokens", usage["cache_write"])

    config = _get_budget_config(user_id)
    daily_limit = config.get("daily_limit_usd", DEFAULT_DAILY_LIMIT)
    monthly_limit = config.get("monthly_limit_usd", DEFAULT_MONTHLY_LIMIT)
    alert_thresholds = config.get("alert_thresholds", [Decimal("0.5"), Decimal("0.8")])

    daily_spend = updated["daily_spend_usd"]
    monthly_spend = updated["monthly_spend_usd"]

    if daily_spend >= daily_limit or monthly_spend >= monthly_limit:
        # _claim_deny is conditional, so exactly one concurrent invocation wins.
        if _claim_deny(user_id):
            _enforce_deny(
                user_id, updated, daily_spend, monthly_spend,
                daily_limit, monthly_limit,
            )
        return True

    _check_thresholds(
        user_id, daily_spend, monthly_spend, daily_limit, monthly_limit,
        alert_thresholds,
    )
    return True


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------


def _try_claim_request(request_id: str) -> bool:
    """
    Attempt to write request_id to the dedup table with a condition that it
    does not already exist. Returns True if claimed (first time), False if dup.
    """
    ttl = int(time.time()) + 86400  # 24h TTL
    try:
        dedup_table.put_item(
            Item={"request_id": request_id, "ttl": ttl},
            ConditionExpression="attribute_not_exists(request_id)",
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


# ---------------------------------------------------------------------------
# Principal parsing
# ---------------------------------------------------------------------------


def _extract_user_id(arn: str) -> str:
    """
    Extract a user identifier from an IAM ARN.

    Handles:
      arn:aws:iam::123456789012:user/username -> username
      arn:aws:sts::123456789012:assumed-role/RoleName/session-name -> session-name
    """
    try:
        resource = arn.split(":", 5)[-1]  # e.g. "user/jdoe" or "assumed-role/MyRole/jdoe"
        parts = resource.split("/")
        if parts[0] == "assumed-role" and len(parts) >= 3:
            return parts[2]  # session name (typically the username)
        elif parts[0] == "user" and len(parts) >= 2:
            return parts[1]
        else:
            return parts[-1]
    except (IndexError, AttributeError):
        return ""


def _extract_principal_info(arn: str) -> dict:
    """
    Extract IAM principal info from an ARN.
    Returns dict with role_name, user_name, and principal_type.

    arn:aws:sts::123456789012:assumed-role/MyRole/session -> role
    arn:aws:iam::123456789012:user/jdoe -> user
    """
    try:
        resource = arn.split(":", 5)[-1]
        parts = resource.split("/")
        if parts[0] == "assumed-role" and len(parts) >= 2:
            return {"role_name": parts[1], "user_name": "", "principal_type": "role"}
        elif parts[0] == "user" and len(parts) >= 2:
            return {"role_name": "", "user_name": parts[1], "principal_type": "user"}
        return {"role_name": "", "user_name": "", "principal_type": "unknown"}
    except (IndexError, AttributeError):
        return {"role_name": "", "user_name": "", "principal_type": "unknown"}


# ---------------------------------------------------------------------------
# Pricing lookup
# ---------------------------------------------------------------------------


def _get_pricing(model_id: str) -> dict | None:
    """Retrieve pricing for a model, using in-memory cache."""
    if model_id in _pricing_cache:
        return _pricing_cache[model_id]

    resp = pricing_table.get_item(Key={"model_id": model_id})
    item = resp.get("Item")
    if item:
        _pricing_cache[model_id] = item
    return item


# ---------------------------------------------------------------------------
# Usage tracking
# ---------------------------------------------------------------------------


def _increment_usage(
    user_id: str,
    cost: Decimal,
    usage: dict,
    timestamp: str,
    identity_arn: str,
) -> dict:
    """
    Atomically increment spend, invocation count and per-component token counters.
    Returns the updated item attributes.

    Token counters are broken out by component so cache activity can be
    reconciled against the Bedrock CacheReadInputTokens / CacheWriteInputTokens
    CloudWatch metrics — an accuracy check independent of this Lambda.
    """
    principal_info = _extract_principal_info(identity_arn)

    resp = usage_table.update_item(
        Key={"user_id": user_id},
        UpdateExpression=(
            "ADD daily_spend_usd :cost, monthly_spend_usd :cost, "
            "daily_invocation_count :one, "
            "daily_input_tokens :in_tok, daily_output_tokens :out_tok, "
            "daily_cache_read_tokens :cr_tok, daily_cache_write_tokens :cw_tok, "
            "daily_cache_write_1h_tokens :cw1h_tok "
            "SET last_invocation_ts = :ts, last_usage_source = :src, "
            "iam_role_name = if_not_exists(iam_role_name, :role), "
            "iam_user_name = if_not_exists(iam_user_name, :iamuser), "
            "principal_type = if_not_exists(principal_type, :ptype)"
        ),
        ExpressionAttributeValues={
            ":cost": cost,
            ":one": 1,
            ":in_tok": usage["input"],
            ":out_tok": usage["output"],
            ":cr_tok": usage["cache_read"],
            ":cw_tok": usage["cache_write"],
            ":cw1h_tok": usage.get("cache_write_1h", 0),
            ":ts": timestamp,
            ":src": usage["source"],
            ":role": principal_info["role_name"],
            ":iamuser": principal_info["user_name"],
            ":ptype": principal_info["principal_type"],
        },
        ReturnValues="ALL_NEW",
    )
    return resp["Attributes"]


# ---------------------------------------------------------------------------
# Budget config (cached)
# ---------------------------------------------------------------------------


def _get_budget_config(user_id: str) -> dict:
    """
    Get budget config for a user, falling back to the DEFAULT entry.

    Cached in-process for BUDGET_CACHE_TTL_SECONDS because this is the hottest
    read on the enforcement path. Budget changes take effect within the TTL.
    """
    now = time.time()
    cached = _budget_cache.get(user_id)
    if cached and (now - cached[0]) < BUDGET_CACHE_TTL_SECONDS:
        return cached[1]

    resp = budget_config_table.get_item(Key={"user_id": user_id})
    item = resp.get("Item")
    if not item:
        resp = budget_config_table.get_item(Key={"user_id": "DEFAULT"})
        item = resp.get("Item", {})

    _budget_cache[user_id] = (now, item)
    return item


# ---------------------------------------------------------------------------
# Metrics (CloudWatch Embedded Metric Format — no extra IAM required)
# ---------------------------------------------------------------------------


def _emit_metric(name: str, value, unit: str = "Count", **dimensions) -> None:
    """
    Emit a CloudWatch metric via Embedded Metric Format.

    EMF is parsed out of the Lambda log stream by CloudWatch, so this needs no
    cloudwatch:PutMetricData permission and adds no latency.
    """
    try:
        payload = {
            "_aws": {
                "Timestamp": int(time.time() * 1000),
                "CloudWatchMetrics": [
                    {
                        "Namespace": METRIC_NAMESPACE,
                        "Dimensions": [list(dimensions.keys())] if dimensions else [[]],
                        "Metrics": [{"Name": name, "Unit": unit}],
                    }
                ],
            },
            name: value,
        }
        payload.update(dimensions)
        print(json.dumps(payload))
    except Exception:  # never let telemetry break enforcement
        logger.debug("Failed to emit metric %s", name, exc_info=True)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _fmt_usd(amount: Decimal) -> str:
    """Format a USD amount with enough precision to be meaningful."""
    if amount < Decimal("0.01"):
        return f"${amount:.8f}"
    elif amount < Decimal("1"):
        return f"${amount:.4f}"
    return f"${amount:.2f}"


# ---------------------------------------------------------------------------
# Deny policy construction
# ---------------------------------------------------------------------------

DENY_ACTIONS = [
    "bedrock:InvokeModel",
    "bedrock:InvokeModelWithResponseStream",
    "bedrock:Converse",
    "bedrock:ConverseStream",
]


def _get_role_id(role_name: str) -> str | None:
    """
    Look up a role's unique ID (AROA...), used to build the aws:userid condition
    value. Cached per execution environment.
    """
    if role_name in _role_id_cache:
        return _role_id_cache[role_name]
    try:
        resp = iam_client.get_role(RoleName=role_name)
        role_id = resp["Role"]["RoleId"]
        _role_id_cache[role_name] = role_id
        return role_id
    except ClientError:
        logger.exception("Failed to get role ID for %s", role_name)
        return None


def _build_deny_policy(user_id: str, principal_type: str, role_name: str) -> str | None:
    """
    Build the deny policy document.

    For assumed roles the statement is scoped with a condition on `aws:userid`,
    whose value for a role session is "<role-unique-id>:<session-name>". Without
    that condition the deny would apply to every session of the role, so one user
    exceeding their budget would block everyone sharing the role.

    Returns None when a role-scoped policy cannot be built and REQUIRE_SCOPED_DENY
    is set, so the caller can decline to attach it.
    """
    statement = {
        "Sid": "BudgetExceededDenyBedrock",
        "Effect": "Deny",
        "Action": DENY_ACTIONS,
        "Resource": "*",
    }

    if principal_type == "role":
        role_id = _get_role_id(role_name) if role_name else None
        if role_id:
            statement["Condition"] = {
                "StringLike": {"aws:userid": f"{role_id}:{user_id}"}
            }
        elif REQUIRE_SCOPED_DENY:
            logger.error(
                "Cannot scope deny policy to session for user=%s role=%s. "
                "Refusing to attach an unscoped deny that would affect every "
                "principal using this role. Set REQUIRE_SCOPED_DENY=false to "
                "override (only safe if roles are single-user).",
                user_id, role_name,
            )
            return None
        else:
            logger.warning(
                "Attaching UNSCOPED deny to role %s — this affects every principal "
                "assuming that role", role_name,
            )

    # IAM users need no condition: the policy attaches to that user alone.
    return json.dumps({"Version": "2012-10-17", "Statement": [statement]})


# ---------------------------------------------------------------------------
# Enforcement
# ---------------------------------------------------------------------------


def _claim_deny(user_id: str) -> bool:
    """
    Atomically claim the right to enforce for this user. Returns True for exactly
    one caller, preventing concurrent invocations from double-enforcing and
    double-alerting.
    """
    try:
        usage_table.update_item(
            Key={"user_id": user_id},
            UpdateExpression="SET is_denied = :t, denied_at = :now",
            ConditionExpression="attribute_not_exists(is_denied) OR is_denied = :f",
            ExpressionAttributeValues={
                ":t": True,
                ":f": False,
                ":now": str(int(time.time())),
            },
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def _release_deny_claim(user_id: str) -> None:
    """Roll back the deny claim when the IAM attach fails, so a later retry can try again."""
    try:
        usage_table.update_item(
            Key={"user_id": user_id},
            UpdateExpression="SET is_denied = :f REMOVE denied_at",
            ExpressionAttributeValues={":f": False},
        )
    except ClientError:
        logger.exception("Failed to release deny claim for user %s", user_id)


def _enforce_deny(
    user_id: str,
    usage_item: dict,
    daily_spend: Decimal,
    monthly_spend: Decimal,
    daily_limit: Decimal,
    monthly_limit: Decimal,
) -> None:
    """Attach the session-scoped IAM deny policy and notify."""
    policy_name = f"BudgetExceeded-{user_id}"
    principal_type = usage_item.get("principal_type", "")
    role_name = usage_item.get("iam_role_name", "")
    iam_user_name = usage_item.get("iam_user_name", "")

    # Infer principal type if the record predates the field being set
    if not principal_type:
        principal_type = "role" if role_name else ("user" if iam_user_name else "unknown")

    policy_document = _build_deny_policy(user_id, principal_type, role_name)
    if policy_document is None:
        _release_deny_claim(user_id)
        _emit_metric("UnscopableDenySkipped", 1)
        _publish(
            subject=f"Budget Exceeded (NOT ENFORCED): {user_id}",
            message=(
                f"Budget EXCEEDED for user: {user_id}\n\n"
                f"Daily spend:   {_fmt_usd(daily_spend)} / {_fmt_usd(daily_limit)} limit\n"
                f"Monthly spend: {_fmt_usd(monthly_spend)} / {_fmt_usd(monthly_limit)} limit\n\n"
                f"NO ENFORCEMENT ACTION WAS TAKEN.\n\n"
                f"The deny policy could not be scoped to this user's session "
                f"(role='{role_name}'). Attaching an unscoped deny would have blocked "
                f"Bedrock for every principal assuming that role.\n\n"
                f"Verify the Lambda has iam:GetRole permission for the role, then "
                f"enforce manually if appropriate."
            ),
        )
        return

    if principal_type == "user" and iam_user_name:
        try:
            iam_client.put_user_policy(
                UserName=iam_user_name,
                PolicyName=policy_name,
                PolicyDocument=policy_document,
            )
            target_desc = f"IAM user '{iam_user_name}'"
            logger.info("Attached deny policy %s to IAM user %s", policy_name, iam_user_name)
        except ClientError:
            logger.exception("Failed to attach deny policy to user %s", iam_user_name)
            _release_deny_claim(user_id)
            _emit_metric("DenyAttachFailures", 1)
            return
    elif role_name:
        try:
            iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName=policy_name,
                PolicyDocument=policy_document,
            )
            target_desc = f"role '{role_name}' (scoped to session '{user_id}')"
            logger.info("Attached scoped deny policy %s to role %s", policy_name, role_name)
        except ClientError:
            logger.exception("Failed to attach deny policy for user %s", user_id)
            _release_deny_claim(user_id)
            _emit_metric("DenyAttachFailures", 1)
            return
    else:
        logger.error(
            "No iam_role_name or iam_user_name for user %s — cannot attach deny policy",
            user_id,
        )
        _release_deny_claim(user_id)
        _emit_metric("DenyAttachFailures", 1)
        return

    _emit_metric("BudgetEnforced", 1)
    _publish(
        subject=f"Budget Exceeded: {user_id}",
        message=(
            f"Budget EXCEEDED for user: {user_id}\n\n"
            f"Daily spend:   {_fmt_usd(daily_spend)} / {_fmt_usd(daily_limit)} limit\n"
            f"Monthly spend: {_fmt_usd(monthly_spend)} / {_fmt_usd(monthly_limit)} limit\n\n"
            f"Token usage today:\n"
            f"  input        : {int(usage_item.get('daily_input_tokens', 0)):,}\n"
            f"  output       : {int(usage_item.get('daily_output_tokens', 0)):,}\n"
            f"  cache read   : {int(usage_item.get('daily_cache_read_tokens', 0)):,}\n"
            f"  cache write  : {int(usage_item.get('daily_cache_write_tokens', 0)):,}"
            f" (1h TTL: {int(usage_item.get('daily_cache_write_1h_tokens', 0)):,})\n\n"
            f"Action: IAM deny policy '{policy_name}' attached to {target_desc}\n"
            f"The user cannot invoke Bedrock models until the next budget reset."
        ),
    )


# ---------------------------------------------------------------------------
# Threshold alerts
# ---------------------------------------------------------------------------


def _claim_alert(user_id: str, label: str) -> bool:
    """
    Atomically record that an alert label has been sent for this user.
    Returns True only for the first caller, so concurrent invocations cannot
    produce duplicate alerts. Cleared by the Budget Reset Lambda.
    """
    try:
        usage_table.update_item(
            Key={"user_id": user_id},
            UpdateExpression="ADD alerts_sent :label",
            ConditionExpression=(
                "attribute_not_exists(alerts_sent) OR NOT contains(alerts_sent, :s)"
            ),
            ExpressionAttributeValues={":label": {label}, ":s": label},
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def _check_thresholds(
    user_id: str,
    daily_spend: Decimal,
    monthly_spend: Decimal,
    daily_limit: Decimal,
    monthly_limit: Decimal,
    thresholds: list[Decimal],
) -> None:
    """
    Fire an alert the first time spend reaches each configured threshold.

    Uses an atomic claim on the usage item rather than inferring a crossing from
    the previous value, which under concurrency could alert twice or not at all.
    """
    for threshold in sorted(thresholds):
        if threshold >= Decimal("1.0"):
            continue  # 100% is handled by enforcement

        pct = int(threshold * 100)

        if daily_spend >= (daily_limit * threshold):
            if _claim_alert(user_id, f"daily:{pct}"):
                _publish_threshold_alert(user_id, "daily", pct, daily_spend, daily_limit)

        if monthly_spend >= (monthly_limit * threshold):
            if _claim_alert(user_id, f"monthly:{pct}"):
                _publish_threshold_alert(user_id, "monthly", pct, monthly_spend, monthly_limit)


def _publish_threshold_alert(
    user_id: str, period: str, pct: int, spend: Decimal, limit: Decimal
) -> None:
    """Publish an SNS alert for a threshold crossing."""
    _publish(
        subject=f"Budget Alert ({pct}%): {user_id}",
        message=(
            f"Budget Alert: {user_id} has reached {pct}% of their {period} Bedrock budget.\n\n"
            f"Current {period} spend: {_fmt_usd(spend)}\n"
            f"{period.capitalize()} limit: {_fmt_usd(limit)}\n\n"
            f"No enforcement action taken yet."
        ),
    )
    logger.info("Threshold alert sent: user=%s period=%s pct=%d%%", user_id, period, pct)


# ---------------------------------------------------------------------------
# SNS
# ---------------------------------------------------------------------------


def _publish(subject: str, message: str) -> None:
    """Publish to the alert topic, swallowing errors so SNS cannot break enforcement."""
    try:
        sns_client.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject[:100], Message=message)
    except ClientError:
        logger.exception("Failed to publish SNS notification: %s", subject)
