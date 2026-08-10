"""
Seed the BedrockModelPricing DynamoDB table with initial model pricing.

This provides a bootstrap set of pricing data so the system works immediately
after deployment, before the first PricingSync Lambda run.

The PricingSync Lambda (scheduled daily) will automatically keep this table
current by pulling from the AWS Price List API + Bedrock ListFoundationModels,
including authoritative prompt-cache read/write rates.

Prompt cache pricing
--------------------
Cached tokens are billed at rates different from uncached input:

  * cache reads  — discounted (roughly 10% of the input rate)
  * cache writes — at a premium (roughly 1.25x the input rate for a 5-minute TTL)

Because the enforcement Lambda meters cache tokens separately, the pricing table
carries explicit cache columns. The values seeded here are DERIVED from those
documented multipliers, not pulled from the Price List API, and are marked
`cache_pricing_source: "derived_multiplier"` so it is obvious they are estimates.
The PricingSync Lambda overwrites them with real rates on its first run.

Models not billed per token (embeddings have no output tokens; image and video
models have no token counts at all) get no cache pricing.

Usage:
    python scripts/seed_pricing.py [--pricing-table BedrockModelPricing] [--region us-east-1]
"""

import argparse
from decimal import Decimal

import boto3

# Cache multipliers relative to the uncached input token rate, keyed by model
# family. These ratios differ by provider and were verified against published
# Price List rates in us-east-1:
#
#   Anthropic   — read 0.10x, 5m write 1.25x, 1h write 2.00x. Identical across
#                 Haiku 4.5 / Sonnet 4.5 / Opus 4.5 and both rate scopes.
#   Amazon Nova — read 0.25x, cache writes are free ($0.00).
#
# Applying one global ratio would invent cache-write charges for Nova that do not
# exist, and underprice its cache reads by 2.5x.
CACHE_MULTIPLIERS_BY_FAMILY = {
    "anthropic": {"read": Decimal("0.10"), "write": Decimal("1.25"), "write_1h": Decimal("2.00")},
    "amazon": {"read": Decimal("0.25"), "write": Decimal("0"), "write_1h": Decimal("0")},
    "_default": {"read": Decimal("0.10"), "write": Decimal("1.25"), "write_1h": Decimal("2.00")},
}

# Models that do not support prompt caching, or are not billed per token.
NO_CACHE_PRICING = {
    "amazon.titan-embed-text-v2:0",
    "amazon.titan-text-express-v1",
    "amazon.titan-text-lite-v1",
}

# Only these Claude models support the 1-hour cache TTL tier. Everything else
# defaults to the 5-minute tier, so seeding a 1h rate would be misleading.
SUPPORTS_1H_CACHE_TTL = (
    "claude-opus-4-5",
    "claude-sonnet-4-5",
    "claude-haiku-4-5",
)


def _model_family(model_id: str) -> str:
    """Provider family from a model ID, ignoring any cross-region prefix."""
    parts = model_id.split(".")
    if parts and parts[0].lower() in ("us", "eu", "ap", "apac"):
        parts = parts[1:]
    return parts[0].lower() if parts else ""


# Bootstrap pricing data covering common models across all providers.
# Prices are per 1K tokens as of mid-2025. The daily PricingSync Lambda
# will overwrite these with current prices from the AWS Price List API.
PRICING_DATA = [
    # --- Anthropic ---
    {"model_id": "anthropic.claude-sonnet-4-v1:0", "input_price_per_1k_tokens": Decimal("0.003"), "output_price_per_1k_tokens": Decimal("0.015")},
    {"model_id": "anthropic.claude-opus-4-v1:0", "input_price_per_1k_tokens": Decimal("0.015"), "output_price_per_1k_tokens": Decimal("0.075")},
    {"model_id": "anthropic.claude-haiku-3-5-v2:0", "input_price_per_1k_tokens": Decimal("0.0008"), "output_price_per_1k_tokens": Decimal("0.004")},
    {"model_id": "anthropic.claude-sonnet-4-5-v1:0", "input_price_per_1k_tokens": Decimal("0.003"), "output_price_per_1k_tokens": Decimal("0.015")},
    {"model_id": "anthropic.claude-3-haiku-20240307-v1:0", "input_price_per_1k_tokens": Decimal("0.00025"), "output_price_per_1k_tokens": Decimal("0.00125")},
    # --- Anthropic cross-region ---
    {"model_id": "us.anthropic.claude-sonnet-4-v1:0", "input_price_per_1k_tokens": Decimal("0.003"), "output_price_per_1k_tokens": Decimal("0.015")},
    {"model_id": "us.anthropic.claude-opus-4-v1:0", "input_price_per_1k_tokens": Decimal("0.015"), "output_price_per_1k_tokens": Decimal("0.075")},
    {"model_id": "us.anthropic.claude-sonnet-4-5-v1:0", "input_price_per_1k_tokens": Decimal("0.003"), "output_price_per_1k_tokens": Decimal("0.015")},
    # --- Amazon Nova ---
    {"model_id": "amazon.nova-micro-v1:0", "input_price_per_1k_tokens": Decimal("0.000035"), "output_price_per_1k_tokens": Decimal("0.00014")},
    {"model_id": "amazon.nova-lite-v1:0", "input_price_per_1k_tokens": Decimal("0.00006"), "output_price_per_1k_tokens": Decimal("0.00024")},
    {"model_id": "amazon.nova-pro-v1:0", "input_price_per_1k_tokens": Decimal("0.0008"), "output_price_per_1k_tokens": Decimal("0.0032")},
    {"model_id": "amazon.nova-premier-v1:0", "input_price_per_1k_tokens": Decimal("0.0025"), "output_price_per_1k_tokens": Decimal("0.0125")},
    # --- Amazon Titan ---
    {"model_id": "amazon.titan-text-express-v1", "input_price_per_1k_tokens": Decimal("0.0002"), "output_price_per_1k_tokens": Decimal("0.0006")},
    {"model_id": "amazon.titan-text-lite-v1", "input_price_per_1k_tokens": Decimal("0.00015"), "output_price_per_1k_tokens": Decimal("0.0002")},
    {"model_id": "amazon.titan-embed-text-v2:0", "input_price_per_1k_tokens": Decimal("0.00002"), "output_price_per_1k_tokens": Decimal("0")},
    # --- Meta Llama ---
    {"model_id": "meta.llama3-3-70b-instruct-v1:0", "input_price_per_1k_tokens": Decimal("0.00072"), "output_price_per_1k_tokens": Decimal("0.00072")},
    {"model_id": "meta.llama3-2-90b-instruct-v1:0", "input_price_per_1k_tokens": Decimal("0.002"), "output_price_per_1k_tokens": Decimal("0.002")},
    {"model_id": "meta.llama3-2-11b-instruct-v1:0", "input_price_per_1k_tokens": Decimal("0.00016"), "output_price_per_1k_tokens": Decimal("0.00016")},
    {"model_id": "meta.llama3-2-3b-instruct-v1:0", "input_price_per_1k_tokens": Decimal("0.00015"), "output_price_per_1k_tokens": Decimal("0.00015")},
    {"model_id": "meta.llama3-2-1b-instruct-v1:0", "input_price_per_1k_tokens": Decimal("0.0001"), "output_price_per_1k_tokens": Decimal("0.0001")},
    {"model_id": "meta.llama3-1-405b-instruct-v1:0", "input_price_per_1k_tokens": Decimal("0.00195"), "output_price_per_1k_tokens": Decimal("0.00256")},
    {"model_id": "meta.llama3-1-70b-instruct-v1:0", "input_price_per_1k_tokens": Decimal("0.00072"), "output_price_per_1k_tokens": Decimal("0.00072")},
    {"model_id": "meta.llama3-1-8b-instruct-v1:0", "input_price_per_1k_tokens": Decimal("0.00022"), "output_price_per_1k_tokens": Decimal("0.00022")},
    {"model_id": "meta.llama4-maverick-17b-instruct-v1:0", "input_price_per_1k_tokens": Decimal("0.0002"), "output_price_per_1k_tokens": Decimal("0.00085")},
    {"model_id": "meta.llama4-scout-17b-instruct-v1:0", "input_price_per_1k_tokens": Decimal("0.00015"), "output_price_per_1k_tokens": Decimal("0.00055")},
    # --- Mistral ---
    {"model_id": "mistral.mistral-large-2407-v1:0", "input_price_per_1k_tokens": Decimal("0.002"), "output_price_per_1k_tokens": Decimal("0.006")},
    {"model_id": "mistral.mistral-7b-instruct-v0:2", "input_price_per_1k_tokens": Decimal("0.00015"), "output_price_per_1k_tokens": Decimal("0.0002")},
    {"model_id": "mistral.mixtral-8x7b-instruct-v0:1", "input_price_per_1k_tokens": Decimal("0.00045"), "output_price_per_1k_tokens": Decimal("0.0007")},
    {"model_id": "mistral.mistral-small-2402-v1:0", "input_price_per_1k_tokens": Decimal("0.001"), "output_price_per_1k_tokens": Decimal("0.003")},
    # --- DeepSeek ---
    {"model_id": "deepseek.r1-v1:0", "input_price_per_1k_tokens": Decimal("0.00135"), "output_price_per_1k_tokens": Decimal("0.0054")},
]


# Default budget config
DEFAULT_BUDGET_CONFIG = {
    "user_id": "DEFAULT",
    "daily_limit_usd": Decimal("50"),
    "monthly_limit_usd": Decimal("500"),
    "alert_thresholds": [Decimal("0.5"), Decimal("0.8")],
    "team": "default",
}


def _with_cache_pricing(item: dict) -> dict:
    """
    Derive prompt-cache prices from the input rate using the model's family ratios.

    Returned values are estimates. The PricingSync Lambda replaces them with
    authoritative rates from the Price List API — for current Claude models those
    come from the `AmazonBedrockFoundationModels` service code, which also
    publishes the 1-hour TTL cache-write rate.
    """
    model_id = item["model_id"]
    if model_id in NO_CACHE_PRICING:
        return item

    multipliers = CACHE_MULTIPLIERS_BY_FAMILY.get(
        _model_family(model_id), CACHE_MULTIPLIERS_BY_FAMILY["_default"]
    )
    input_price = item["input_price_per_1k_tokens"]

    seeded = {
        **item,
        "cache_read_price_per_1k_tokens": (input_price * multipliers["read"]).normalize(),
        "cache_write_price_per_1k_tokens": (input_price * multipliers["write"]).normalize(),
        "cache_pricing_source": "derived_multiplier",
    }

    if any(marker in model_id for marker in SUPPORTS_1H_CACHE_TTL):
        seeded["cache_write_1h_price_per_1k_tokens"] = (
            input_price * multipliers["write_1h"]
        ).normalize()

    return seeded


def seed_pricing(table_name: str, region: str) -> None:
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    print(f"Seeding {len(PRICING_DATA)} pricing records into {table_name}...")
    with_cache = 0
    for item in PRICING_DATA:
        item_with_date = {
            **_with_cache_pricing(item),
            "effective_date": "2025-06-01",
            "source": "manual_seed",
        }
        table.put_item(Item=item_with_date)
        has_cache = "cache_read_price_per_1k_tokens" in item_with_date
        with_cache += 1 if has_cache else 0
        print(f"  + {item['model_id']}{'  (+cache rates)' if has_cache else ''}")

    print(f"\nDone. {len(PRICING_DATA)} models seeded, {with_cache} with cache pricing.")
    print(
        "Cache rates are derived estimates. Run the PricingSync Lambda to replace\n"
        "them with authoritative rates from the AWS Price List API:\n"
        "  aws lambda invoke --function-name BedrockPricingSync --payload '{}' \\\n"
        "    --cli-binary-format raw-in-base64-out /dev/stdout"
    )


def seed_default_budget(table_name: str, region: str) -> None:
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    print(f"\nSeeding DEFAULT budget config into {table_name}...")
    table.put_item(Item=DEFAULT_BUDGET_CONFIG)
    print(f"  + DEFAULT (daily=${DEFAULT_BUDGET_CONFIG['daily_limit_usd']}, monthly=${DEFAULT_BUDGET_CONFIG['monthly_limit_usd']})")
    print("Done.")


def main():
    parser = argparse.ArgumentParser(description="Seed Bedrock pricing and budget config tables")
    parser.add_argument("--pricing-table", default="BedrockModelPricing", help="Pricing table name")
    parser.add_argument("--budget-table", default="BedrockBudgetConfig", help="Budget config table name")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument("--skip-budget", action="store_true", help="Skip seeding budget config")
    args = parser.parse_args()

    seed_pricing(args.pricing_table, args.region)
    if not args.skip_budget:
        seed_default_budget(args.budget_table, args.region)


if __name__ == "__main__":
    main()
