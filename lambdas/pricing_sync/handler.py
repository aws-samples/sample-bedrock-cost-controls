"""
Pricing Sync Lambda — scheduled daily via EventBridge.

Pulls current Bedrock model pricing from the AWS Price List API and
reconciles it with model IDs from the Bedrock ListFoundationModels API.

TWO PRICING SOURCES
-------------------
Bedrock pricing is split across two Price List service codes with different
schemas. Querying only the first misses every current-generation Anthropic model.

1. ``AmazonBedrock`` — first-party and older third-party models: Amazon Nova and
   Titan, Meta Llama, Mistral, DeepSeek, and legacy Claude (2.x, 3 Sonnet,
   3 Haiku, Instant). Keyed by an ``inferenceType`` attribute, priced per
   **1K tokens**, usagetypes like ``USE1-NovaPro-cache-read-input-token-count``.

2. ``AmazonBedrockFoundationModels`` — marketplace-style listings covering
   current Claude (Sonnet / Opus / Haiku 4.x and 5.x), Cohere, Jamba, Palmyra,
   Stability, TwelveLabs and others. No ``inferenceType``; models are keyed by
   ``servicename`` such as "Claude Sonnet 4.5 (Amazon Bedrock Edition)", priced
   per **1M tokens**, usagetypes like
   ``USE1-MP:USE1_CacheReadInputTokenCount_Global-Units``.

Source 2 is preferred when both match, because it carries the current model
generations and publishes explicit prompt-cache rates including the 1-hour TTL
tier that source 1 does not expose.

REGIONAL VS GLOBAL RATES
------------------------
Source 2 publishes two rate scopes roughly 10% apart:

* **Regional** — a bare model ID invoked in-Region, e.g. ``anthropic.claude-...``
* **Global** — a cross-region inference profile, e.g. ``us.anthropic.claude-...``

``RATE_SCOPE`` selects which to store and defaults to ``global``, assuming callers
use cross-region inference profiles (the common pattern in the US). Set it to
``regional`` if traffic uses bare model IDs pinned to one Region.

Strategy:
1. Call Bedrock ListFoundationModels to get all available model IDs
2. Fetch pricing from both service codes
3. Match each model, preferring the foundation-models source
4. Write per-component prices (input, output, cache read, cache write 5m, cache
   write 1h) to the BedrockModelPricing DynamoDB table
"""

import json
import logging
import os
import re
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Clients ---
dynamodb = boto3.resource("dynamodb")
bedrock_client = boto3.client("bedrock")
pricing_client = boto3.client("pricing", region_name="us-east-1")  # Pricing API only in us-east-1

# --- Environment ---
PRICING_TABLE = os.environ["PRICING_TABLE"]
REGION = os.environ.get("AWS_REGION", "us-east-1")

# "global" (cross-region inference profiles) or "regional" (bare model IDs).
RATE_SCOPE = (os.environ.get("RATE_SCOPE") or "global").strip().lower()
if RATE_SCOPE not in ("global", "regional"):
    logger.warning("Invalid RATE_SCOPE=%r, falling back to 'global'", RATE_SCOPE)
    RATE_SCOPE = "global"

LEGACY_SERVICE_CODE = "AmazonBedrock"
FM_SERVICE_CODE = "AmazonBedrockFoundationModels"

pricing_table = dynamodb.Table(PRICING_TABLE)


def lambda_handler(event: dict, context) -> dict:
    """Entry point for scheduled pricing sync."""
    logger.info("Starting pricing sync")

    # Step 1: Get all foundation models from Bedrock
    models = _list_all_models()
    logger.info("Found %d foundation models from Bedrock API", len(models))

    # Step 2: Get on-demand token pricing from BOTH Price List service codes.
    # The legacy code covers Nova/Titan/Llama/Mistral/DeepSeek and older Claude;
    # the foundation-models code covers current Claude, Cohere, Jamba and others.
    legacy_records = _fetch_all_pricing()
    fm_records = _fetch_fm_pricing()
    logger.info(
        "Fetched pricing records: %s=%d %s=%d",
        LEGACY_SERVICE_CODE, len(legacy_records), FM_SERVICE_CODE, len(fm_records),
    )

    # Step 3: Build lookups for each source
    price_map = _build_price_map(legacy_records)
    fm_price_map = _build_fm_price_map(fm_records)
    logger.info(
        "Built price maps: legacy_keys=%d fm_keys=%d rate_scope=%s",
        len(price_map), len(fm_price_map), RATE_SCOPE,
    )

    # Step 4: Match models to pricing and write to DynamoDB
    updated = 0
    with_cache_pricing = 0
    with_1h_cache_pricing = 0
    by_source = {"foundation_models": 0, "legacy": 0}
    without_cache_pricing = []
    unmatched = []

    for model in models:
        model_id = model["modelId"]

        # Prefer the foundation-models source: it carries the current generations
        # and publishes explicit cache rates including the 1-hour TTL tier.
        prices, scope = _match_fm_pricing(model, fm_price_map)
        source = FM_SERVICE_CODE
        if not prices:
            prices = _match_model_pricing(model_id, model, price_map)
            scope = ""
            source = LEGACY_SERVICE_CODE

        if prices:
            _write_pricing(model_id, prices, source=source, scope=scope)
            updated += 1
            by_source["foundation_models" if source == FM_SERVICE_CODE else "legacy"] += 1
            if "cache_read" in prices or "cache_write" in prices:
                with_cache_pricing += 1
            else:
                without_cache_pricing.append(model_id)
            if "cache_write_1h" in prices:
                with_1h_cache_pricing += 1
        else:
            unmatched.append(model_id)

    # Also write cross-region inference variants (us.*, eu.*)
    cross_region_updated = _sync_cross_region_variants(models, price_map)

    logger.info(
        "Pricing sync complete: updated=%d (fm=%d legacy=%d) with_cache=%d "
        "with_1h_cache=%d cross_region=%d unmatched=%d rate_scope=%s",
        updated, by_source["foundation_models"], by_source["legacy"],
        with_cache_pricing, with_1h_cache_pricing, cross_region_updated,
        len(unmatched), RATE_SCOPE,
    )
    if unmatched:
        logger.warning("Unmatched models (no pricing found): %s", unmatched[:20])
    if without_cache_pricing:
        # Not an error. These models fall back to the per-family cache multipliers
        # in the enforcement Lambda, which are estimates rather than real rates.
        logger.info(
            "Models with no cache rates in the Price List API (multiplier fallback "
            "applies): %s", without_cache_pricing[:20],
        )

    return {
        "updated": updated,
        "rate_scope": RATE_SCOPE,
        "by_source": by_source,
        "with_cache_pricing": with_cache_pricing,
        "with_1h_cache_pricing": with_1h_cache_pricing,
        "without_cache_pricing": len(without_cache_pricing),
        "cross_region_variants": cross_region_updated,
        "unmatched": len(unmatched),
        "unmatched_models": unmatched[:20],
    }


# ---------------------------------------------------------------------------
# Bedrock model listing
# ---------------------------------------------------------------------------


def _list_all_models() -> list[dict]:
    """List all foundation models available in the account's region."""
    models = []
    try:
        resp = bedrock_client.list_foundation_models()
        models = resp.get("modelSummaries", [])
    except ClientError:
        logger.exception("Failed to list foundation models")
    return models


# ---------------------------------------------------------------------------
# Price List API
# ---------------------------------------------------------------------------


def _fetch_products(service_code: str, filters: list[dict]) -> list[dict]:
    """Page through GetProducts for a service code, returning parsed records."""
    records = []
    next_token = None

    while True:
        kwargs = {
            "ServiceCode": service_code,
            "Filters": filters,
            "MaxResults": 100,
            "FormatVersion": "aws_v1",
        }
        if next_token:
            kwargs["NextToken"] = next_token

        try:
            resp = pricing_client.get_products(**kwargs)
        except ClientError:
            logger.exception("Failed to fetch pricing for %s", service_code)
            break

        for price_str in resp.get("PriceList", []):
            price_item = json.loads(price_str) if isinstance(price_str, str) else price_str
            records.append(price_item)

        next_token = resp.get("NextToken")
        if not next_token:
            break

    return records


def _fetch_all_pricing() -> list[dict]:
    """
    Fetch on-demand token pricing from the `AmazonBedrock` service code for the
    current Region. Covers Nova, Titan, Llama, Mistral, DeepSeek and legacy Claude.
    """
    return _fetch_products(
        LEGACY_SERVICE_CODE,
        [
            {"Type": "TERM_MATCH", "Field": "productFamily", "Value": "Amazon Bedrock"},
            {"Type": "TERM_MATCH", "Field": "location", "Value": _region_to_location(REGION)},
        ],
    )


def _fetch_fm_pricing() -> list[dict]:
    """
    Fetch on-demand token pricing from the `AmazonBedrockFoundationModels` service
    code for the current Region. This is where current-generation Claude lives,
    along with Cohere, Jamba, Palmyra and others.

    Note there is no `productFamily` attribute on this service code, so location is
    the only filter available.
    """
    return _fetch_products(
        FM_SERVICE_CODE,
        [{"Type": "TERM_MATCH", "Field": "location", "Value": _region_to_location(REGION)}],
    )


def _region_to_location(region: str) -> str:
    """Map AWS region code to Price List location name."""
    region_map = {
        "us-east-1": "US East (N. Virginia)",
        "us-east-2": "US East (Ohio)",
        "us-west-1": "US West (N. California)",
        "us-west-2": "US West (Oregon)",
        "eu-west-1": "EU (Ireland)",
        "eu-west-2": "EU (London)",
        "eu-west-3": "EU (Paris)",
        "eu-central-1": "EU (Frankfurt)",
        "eu-central-2": "EU (Zurich)",
        "eu-north-1": "EU (Stockholm)",
        "eu-south-1": "EU (Milan)",
        "ap-northeast-1": "Asia Pacific (Tokyo)",
        "ap-northeast-2": "Asia Pacific (Seoul)",
        "ap-northeast-3": "Asia Pacific (Osaka)",
        "ap-southeast-1": "Asia Pacific (Singapore)",
        "ap-southeast-2": "Asia Pacific (Sydney)",
        "ap-southeast-3": "Asia Pacific (Jakarta)",
        "ap-southeast-4": "Asia Pacific (Melbourne)",
        "ap-southeast-5": "Asia Pacific (Malaysia)",
        "ap-south-1": "Asia Pacific (Mumbai)",
        "ap-south-2": "Asia Pacific (Hyderabad)",
        "sa-east-1": "South America (Sao Paulo)",
        "ca-central-1": "Canada (Central)",
        "me-south-1": "Middle East (Bahrain)",
        "me-central-1": "Middle East (UAE)",
        "af-south-1": "Africa (Cape Town)",
        "il-central-1": "Israel (Tel Aviv)",
    }
    return region_map.get(region, "US East (N. Virginia)")


# ---------------------------------------------------------------------------
# Price mapping
# ---------------------------------------------------------------------------


# Inference types we ingest, mapped to the price component they represent.
# The Price List API publishes prompt cache rates as first-class inference types,
# so cache pricing does not have to be estimated from multipliers.
#
# Deliberately excluded: the "flex" and "priority" service-tier variants, which
# have their own rates and would otherwise collide on the same model key.
INFERENCE_TYPE_TO_COMPONENT = {
    "Input tokens": "input",
    "Output tokens": "output",
    "Prompt cache read input tokens": "cache_read",
    "Prompt cache write input tokens": "cache_write",
}

# Usagetype suffixes to strip when deriving the model key. Order matters: the
# cache suffixes contain the word "input" and must be tried before the plain
# input/output patterns.
_KEY_SUFFIX_PATTERNS = [
    r"-cache-read-input-token-count$",
    r"-cache-write-input-token-count$",
    r"-cache-read-tokens$",
    r"-cache-write-tokens$",
    r"-(input|output)-tokens$",
    r"-(input|output)-token-count$",
]


def _usagetype_to_model_key(normalized: str) -> str:
    """Strip the token-component suffix from a region-stripped usagetype."""
    for pattern in _KEY_SUFFIX_PATTERNS:
        stripped = re.sub(pattern, "", normalized, flags=re.IGNORECASE)
        if stripped != normalized:
            return stripped
    return normalized


def _build_price_map(records: list[dict]) -> dict:
    """
    Build a lookup from model key -> {component: price_per_1k_tokens}, where
    component is one of input, output, cache_read, cache_write.
    """
    price_map = {}

    for record in records:
        product = record.get("product", {})
        attributes = product.get("attributes", {})
        usagetype = attributes.get("usagetype", "")
        inference_type = attributes.get("inferenceType", "")

        usagetype_lower = usagetype.lower()

        # Skip batch, custom model, and cross-region-global pricing. Cross-region
        # variants are handled separately by _sync_cross_region_variants, which
        # copies the base model's pricing; ingesting them here would produce a
        # duplicate model key that overwrites the base rate.
        if (
            "batch" in usagetype_lower
            or "custom-model" in usagetype_lower
            or "cross-region-global" in usagetype_lower
            # Models served on the bedrock-mantle endpoint are not captured by
            # model invocation logging, so this solution cannot meter them.
            # Ingesting their rates would be misleading.
            or "-mantle-" in usagetype_lower
        ):
            continue

        component = INFERENCE_TYPE_TO_COMPONENT.get(inference_type)
        if component is None:
            continue

        # Extract price from on-demand terms
        terms = record.get("terms", {})
        on_demand = terms.get("OnDemand", {})
        price = _extract_price(on_demand)
        if price is None:
            continue

        # Strip region prefix, e.g. "USE1-NovaLite-input-tokens" -> "NovaLite-input-tokens"
        normalized = _strip_region_prefix(usagetype)
        model_key = _usagetype_to_model_key(normalized)

        price_map.setdefault(model_key, {})[component] = price

    return price_map


def _extract_price(on_demand: dict) -> Decimal | None:
    """Extract USD price per unit from OnDemand pricing terms."""
    price, _unit = _extract_price_and_unit(on_demand)
    return price


def _extract_price_and_unit(on_demand: dict) -> tuple[Decimal | None, str]:
    """
    Extract USD price and its unit string from OnDemand pricing terms.

    The unit matters: the `AmazonBedrock` service code quotes "1K tokens" while
    `AmazonBedrockFoundationModels` quotes "1M tokens". Mixing them without
    normalizing would misprice by 1000x.

    A price of exactly zero is returned rather than skipped, because some rates are
    legitimately free — Amazon Nova publishes $0.00 for cache writes, and treating
    that as "missing" would wrongly fall back to a non-zero multiplier estimate.
    """
    for _term_key, term_data in on_demand.items():
        for _dim_key, dim_data in term_data.get("priceDimensions", {}).items():
            price_str = dim_data.get("pricePerUnit", {}).get("USD")
            if price_str is None:
                continue
            return Decimal(price_str), dim_data.get("unit", "")
    return None, ""


def _normalize_to_per_1k(price: Decimal, unit: str) -> Decimal:
    """Convert a price quoted per 1M or per 1K tokens into a per-1K-token price."""
    u = (unit or "").lower()
    if "1m" in u or "million" in u:
        return price / Decimal("1000")
    if "1k" in u or "thousand" in u:
        return price
    logger.warning("Unrecognized price unit %r, assuming per-1K tokens", unit)
    return price


def _strip_region_prefix(usagetype: str) -> str:
    """Strip the region prefix from a usagetype string."""
    # Pattern: 3-4 uppercase chars + digit, then dash
    match = re.match(r"^[A-Z]{2,4}\d?-(.+)$", usagetype)
    return match.group(1) if match else usagetype


# ---------------------------------------------------------------------------
# Foundation-models price map (AmazonBedrockFoundationModels)
# ---------------------------------------------------------------------------

# Component names must be matched longest-first: "CacheWrite1hInputTokenCount"
# shares a prefix with "CacheWriteInputTokenCount".
_FM_USAGETYPE_RE = re.compile(
    r"MP:[A-Za-z0-9]+_"
    r"(?P<component>CacheWrite1hInputTokenCount"
    r"|CacheWriteInputTokenCount"
    r"|CacheReadInputTokenCount"
    r"|InputTokenCount"
    r"|OutputTokenCount)"
    r"(?P<scope>_Global)?"
    r"(?P<batch>_Batch)?"
    r"-Units$"
)

_FM_COMPONENTS = {
    "InputTokenCount": "input",
    "OutputTokenCount": "output",
    "CacheReadInputTokenCount": "cache_read",
    "CacheWriteInputTokenCount": "cache_write",
    "CacheWrite1hInputTokenCount": "cache_write_1h",
}


def _normalize_model_name(name: str) -> str:
    """
    Reduce a model name or Price List `servicename` to a comparable key.

    "Claude Sonnet 4.5 (Amazon Bedrock Edition)" -> "claudesonnet45"
    "Claude Sonnet 4.5"                          -> "claudesonnet45"

    Matching on model *name* rather than model ID is deliberate: IDs carry date
    stamps and version suffixes that change per release, while the display name
    tracks the Price List `servicename` closely.
    """
    if not name:
        return ""
    name = re.sub(r"\s*\(Amazon Bedrock Edition\)\s*$", "", name, flags=re.IGNORECASE)
    return re.sub(r"[^a-z0-9]", "", name.lower())


def _build_fm_price_map(records: list[dict]) -> dict:
    """
    Build {normalized_model_name: {scope: {component: price_per_1k_tokens}}}.

    Batch rates and Reserved / Provisioned TPM rates are excluded — this solution
    meters on-demand token usage only.
    """
    price_map: dict[str, dict] = {}

    for record in records:
        attributes = record.get("product", {}).get("attributes", {})
        usagetype = attributes.get("usagetype", "")
        servicename = attributes.get("servicename", "")

        match = _FM_USAGETYPE_RE.search(usagetype)
        if not match:
            continue
        if match.group("batch"):
            continue

        component = _FM_COMPONENTS[match.group("component")]
        scope = "global" if match.group("scope") else "regional"

        price, unit = _extract_price_and_unit(record.get("terms", {}).get("OnDemand", {}))
        if price is None:
            continue

        key = _normalize_model_name(servicename)
        if not key:
            continue

        price_map.setdefault(key, {}).setdefault(scope, {})[component] = (
            _normalize_to_per_1k(price, unit)
        )

    return price_map


def _match_fm_pricing(model_info: dict, fm_price_map: dict) -> tuple[dict | None, str]:
    """
    Match a Bedrock model to the foundation-models price map by display name.

    Returns (component->price dict, scope_used). Prefers RATE_SCOPE, falling back
    to the other scope when the preferred one publishes no usable rates.
    """
    key = _normalize_model_name(model_info.get("modelName", ""))
    if not key:
        return None, ""

    scopes = fm_price_map.get(key)
    if not scopes:
        return None, ""

    for scope in (RATE_SCOPE, "global", "regional"):
        prices = scopes.get(scope)
        if prices and _usable(prices):
            return prices, scope

    return None, ""


# ---------------------------------------------------------------------------
# Model matching
# ---------------------------------------------------------------------------

# Mapping from Bedrock model ID patterns to usagetype model keys
# This handles the translation between model IDs (e.g., "meta.llama3-70b-instruct-v1:0")
# and pricing usagetype keys (e.g., "Llama3-70B")
MODEL_ID_TO_PRICE_KEY_PATTERNS = [
    # Anthropic
    (r"anthropic\.claude-3-haiku", "Claude3Haiku"),
    (r"anthropic\.claude-3-sonnet", "Claude3Sonnet"),
    (r"anthropic\.claude-v2:1", "Claude2.1"),
    (r"anthropic\.claude-v2", "Claude2.0"),
    (r"anthropic\.claude-instant", "ClaudeInstant"),
    # Amazon Nova
    (r"amazon\.nova-lite", "NovaLite"),
    (r"amazon\.nova-micro", "NovaMicro"),
    (r"amazon\.nova-pro", "NovaPro"),
    (r"amazon\.nova-premier", "NovaPremier"),
    # Amazon Titan
    (r"amazon\.titan-embed-text-v2", "TitanEmbeddingV2-Text"),
    (r"amazon\.titan-embed-text-v1", "TitanEmbeddingsG1-Text"),
    (r"amazon\.titan-embed-image-v1", "TitanEmbeddingsG1-Image"),
    (r"amazon\.titan-text-express", "TitanTextG1-Express"),
    (r"amazon\.titan-text-lite", "TitanTextG1-Lite"),
    (r"amazon\.titan-text-premier", "TitanText-Premier"),
    # Meta Llama
    (r"meta\.llama3-3-70b", "Llama3-3-70B"),
    (r"meta\.llama3-2-90b", "Llama3-2-90B"),
    (r"meta\.llama3-2-11b", "Llama3-2-11B"),
    (r"meta\.llama3-2-3b", "Llama3-2-3B"),
    (r"meta\.llama3-2-1b", "Llama3-2-1B"),
    (r"meta\.llama3-1-405b", "Llama3-1-405B"),
    (r"meta\.llama3-1-70b", "Llama3-1-70B"),
    (r"meta\.llama3-1-8b", "Llama3-1-8B"),
    (r"meta\.llama3-70b", "Llama3-70B"),
    (r"meta\.llama3-8b", "Llama3-8B"),
    (r"meta\.llama4-maverick", "Llama4-Maverick-17B"),
    (r"meta\.llama4-scout", "Llama4-Scout-17B"),
    # Mistral
    (r"mistral\.mistral-7b", "Mistral7B"),
    (r"mistral\.mixtral-8x7b", "Mixtral8x7B"),
    (r"mistral\.mistral-large-2407", "MistralLarge2407"),
    (r"mistral\.mistral-large-2402", "MistralLarge"),
    (r"mistral\.mistral-small", "MistralSmall"),
    # DeepSeek
    (r"deepseek\.r1", "DeepSeek-R1"),
    (r"deepseek\.v3\.2", "deepseek.v3.2"),
    (r"deepseek\.v3\.1", "DeepSeek-V3.1"),
]


def _usable(entry: dict) -> bool:
    """
    A price entry is usable once it has both required token directions.

    Input must be strictly positive: a $0.00 input rate means a placeholder or
    unavailable SKU, and accepting it would silently zero out all cost for that
    model. Output may legitimately be $0.00 (embeddings models produce no billable
    output tokens), and cache components may legitimately be $0.00 (Amazon Nova
    does not charge for cache writes).
    """
    return (
        "input" in entry
        and entry["input"] > 0
        and "output" in entry
    )


def _match_model_pricing(model_id: str, model_info: dict, price_map: dict) -> dict | None:
    """
    Attempt to match a Bedrock model ID to its pricing in the price map.

    Returns the component->price dict (input, output, and cache_read /
    cache_write when the Price List API publishes them), or None if unmatched.
    """
    # Strategy 1: Direct pattern matching from our known mapping
    for pattern, price_key in MODEL_ID_TO_PRICE_KEY_PATTERNS:
        if re.search(pattern, model_id, re.IGNORECASE):
            entry = price_map.get(price_key)
            if entry and _usable(entry):
                return entry

    # Strategy 2: Try fuzzy matching based on model name from Bedrock API
    model_name = model_info.get("modelName", "")
    if model_name:
        normalized_name = model_name.replace(" ", "").replace("-", "")
        for price_key, prices in price_map.items():
            normalized_key = price_key.replace("-", "").replace(".", "")
            if normalized_name.lower() == normalized_key.lower() and _usable(prices):
                return prices

    # Strategy 3: Try matching by provider.model-name pattern in usagetype keys
    # Some usagetypes use the full model ID format (e.g., "deepseek.v3.2-input-tokens")
    model_id_base = model_id.split(":")[0]  # Remove version suffix
    for price_key, prices in price_map.items():
        if price_key.lower() == model_id_base.lower() and _usable(prices):
            return prices

    return None


# ---------------------------------------------------------------------------
# Cross-region inference variants
# ---------------------------------------------------------------------------


def _sync_cross_region_variants(models: list[dict], price_map: dict) -> int:
    """
    For models that support cross-region inference (us.*, eu.*, ap.*),
    copy the base model's pricing to the cross-region model ID.
    """
    count = 0
    # Get all existing pricing from DynamoDB
    existing = set()
    scan_kwargs = {"ProjectionExpression": "model_id"}
    while True:
        resp = pricing_table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            existing.add(item["model_id"])
        if "LastEvaluatedKey" not in resp:
            break
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    # For each model, check if cross-region variants should be created
    prefixes = ["us.", "eu.", "ap."]
    for model_id in list(existing):
        # Skip if already a cross-region variant
        if any(model_id.startswith(p) for p in prefixes):
            continue

        # Check if cross-region variants should exist
        for prefix in prefixes:
            variant_id = f"{prefix}{model_id}"
            if variant_id not in existing:
                # Copy pricing from base model
                try:
                    resp = pricing_table.get_item(Key={"model_id": model_id})
                    item = resp.get("Item")
                    if item:
                        item["model_id"] = variant_id
                        pricing_table.put_item(Item=item)
                        count += 1
                except ClientError:
                    pass

    return count


# ---------------------------------------------------------------------------
# DynamoDB write
# ---------------------------------------------------------------------------


def _write_pricing(model_id: str, prices: dict, source: str = "", scope: str = "") -> None:
    """
    Write or update pricing for a model in DynamoDB.

    Cache columns are written only when the Price List API published them. When
    absent, the enforcement Lambda falls back to the per-family cache multipliers
    rather than pricing cache tokens at zero.

    `cache_write_1h_price_per_1k_tokens` is only published by the foundation-models
    service code, and only for models that support the 1-hour TTL tier.
    """
    item = {
        "model_id": model_id,
        "input_price_per_1k_tokens": prices["input"],
        "output_price_per_1k_tokens": prices["output"],
        "effective_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "last_synced": datetime.now(timezone.utc).isoformat(),
        "source": "price_list_api",
        "pricing_service_code": source or LEGACY_SERVICE_CODE,
    }
    if scope:
        item["rate_scope"] = scope

    for component, column in (
        ("cache_read", "cache_read_price_per_1k_tokens"),
        ("cache_write", "cache_write_price_per_1k_tokens"),
        ("cache_write_1h", "cache_write_1h_price_per_1k_tokens"),
    ):
        if component in prices:
            item[column] = prices[component]

    if any(c in prices for c in ("cache_read", "cache_write", "cache_write_1h")):
        item["cache_pricing_source"] = "price_list_api"

    pricing_table.put_item(Item=item)
