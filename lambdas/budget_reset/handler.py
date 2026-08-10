"""
Budget Reset Lambda — triggered by EventBridge scheduled rules.

Responsibilities:
1. Scan the Usage table for users with non-zero accumulators
2. Reset daily and/or monthly spend to 0
3. Remove IAM deny policies for users who were blocked
4. Publish a summary report to SNS
"""

import json
import logging
import os
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
USAGE_TABLE = os.environ["USAGE_TABLE"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]

# --- Table references ---
usage_table = dynamodb.Table(USAGE_TABLE)


def lambda_handler(event: dict, context) -> dict:
    """
    Entry point. Event contains {"reset_type": "daily"|"monthly"}.
    Monthly resets also clear daily accumulators.
    """
    reset_type = event.get("reset_type", "daily")
    logger.info("Budget reset triggered: type=%s", reset_type)

    # Scan all users with spend > 0
    users = _scan_users_for_reset(reset_type)
    logger.info("Found %d user(s) to reset", len(users))

    reset_count = 0
    unlocked_count = 0
    total_spend_reset = Decimal("0")
    errors = []

    for user in users:
        user_id = user["user_id"]
        try:
            spend_reset = _reset_user(user, reset_type)
            total_spend_reset += spend_reset
            reset_count += 1

            # If user had a deny policy, remove it
            if user.get("is_denied"):
                _remove_deny_policy(user_id, user.get("iam_role_name", ""))
                unlocked_count += 1

        except Exception as e:
            logger.exception("Error resetting user %s", user_id)
            errors.append({"user_id": user_id, "error": str(e)})

    # Publish summary
    _publish_summary(reset_type, reset_count, unlocked_count, total_spend_reset, errors)

    result = {
        "reset_type": reset_type,
        "users_reset": reset_count,
        "users_unlocked": unlocked_count,
        "total_spend_reset_usd": str(total_spend_reset),
        "errors": len(errors),
    }
    logger.info("Reset complete: %s", json.dumps(result))
    return result


# ---------------------------------------------------------------------------
# Scanning
# ---------------------------------------------------------------------------


def _scan_users_for_reset(reset_type: str) -> list[dict]:
    """
    Scan the usage table for users that need resetting.
    For daily: users with daily_spend_usd > 0
    For monthly: users with monthly_spend_usd > 0
    """
    if reset_type == "monthly":
        filter_expr = "monthly_spend_usd > :zero OR daily_spend_usd > :zero"
    else:
        filter_expr = "daily_spend_usd > :zero"

    items = []
    scan_kwargs = {
        "FilterExpression": filter_expr,
        "ExpressionAttributeValues": {":zero": Decimal("0")},
        # Only the attributes the reset actually needs. A filtered Scan reads
        # every item before filtering, so projecting keeps the payload small.
        "ProjectionExpression": (
            "user_id, daily_spend_usd, monthly_spend_usd, is_denied, "
            "iam_role_name, iam_user_name, principal_type"
        ),
    }

    while True:
        resp = usage_table.scan(**scan_kwargs)
        items.extend(resp.get("Items", []))

        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_key

    return items


# ---------------------------------------------------------------------------
# Reset logic
# ---------------------------------------------------------------------------


def _reset_user(user: dict, reset_type: str) -> Decimal:
    """
    Reset a user's accumulators and return the total spend that was reset.
    """
    user_id = user["user_id"]
    spend_reset = Decimal("0")

    # Per-component token counters, reset alongside the daily spend accumulator.
    # These support reconciliation against the Bedrock CacheReadInputTokens /
    # CacheWriteInputTokens CloudWatch metrics.
    counter_reset_clause = (
        "daily_input_tokens = :zero_int, daily_output_tokens = :zero_int, "
        "daily_cache_read_tokens = :zero_int, daily_cache_write_tokens = :zero_int"
    )

    # `alerts_sent` records which threshold alerts have already fired so the
    # enforcement Lambda cannot send duplicates. It MUST be cleared on reset or
    # each user would only ever receive one alert per threshold, permanently.
    clear_alerts = " REMOVE alerts_sent"

    if reset_type == "monthly":
        # Monthly reset clears both daily and monthly
        spend_reset = user.get("monthly_spend_usd", Decimal("0"))
        usage_table.update_item(
            Key={"user_id": user_id},
            UpdateExpression=(
                "SET daily_spend_usd = :zero, monthly_spend_usd = :zero, "
                "daily_invocation_count = :zero_int, is_denied = :f, "
                + counter_reset_clause
                + clear_alerts
            ),
            ExpressionAttributeValues={
                ":zero": Decimal("0"),
                ":zero_int": 0,
                ":f": False,
            },
        )
    else:
        # Daily reset clears only daily accumulators
        spend_reset = user.get("daily_spend_usd", Decimal("0"))
        usage_table.update_item(
            Key={"user_id": user_id},
            UpdateExpression=(
                "SET daily_spend_usd = :zero, daily_invocation_count = :zero_int, "
                "is_denied = :f, "
                + counter_reset_clause
                + clear_alerts
            ),
            ExpressionAttributeValues={
                ":zero": Decimal("0"),
                ":zero_int": 0,
                ":f": False,
            },
        )

    return spend_reset


# ---------------------------------------------------------------------------
# IAM deny policy removal
# ---------------------------------------------------------------------------


def _remove_deny_policy(user_id: str, iam_role_name: str) -> None:
    """Remove the BudgetExceeded inline deny policy from the user's IAM role or user."""
    policy_name = f"BudgetExceeded-{user_id}"

    if iam_role_name:
        try:
            iam_client.delete_role_policy(
                RoleName=iam_role_name,
                PolicyName=policy_name,
            )
            logger.info("Removed deny policy %s from role %s", policy_name, iam_role_name)
            return
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchEntity":
                logger.info("Deny policy %s not found on role %s (already removed)", policy_name, iam_role_name)
            else:
                raise

    # Try removing from IAM user (for principals that are IAM users, not roles)
    # The user_id itself may be the IAM user name
    try:
        iam_client.delete_user_policy(
            UserName=user_id,
            PolicyName=policy_name,
        )
        logger.info("Removed deny policy %s from IAM user %s", policy_name, user_id)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchEntity":
            logger.info("Deny policy %s not found on user %s (already removed or not applicable)", policy_name, user_id)
        else:
            # Don't raise — this is best-effort for user policies
            logger.warning("Could not remove deny policy from user %s: %s", user_id, e)


# ---------------------------------------------------------------------------
# SNS summary
# ---------------------------------------------------------------------------


def _publish_summary(
    reset_type: str,
    reset_count: int,
    unlocked_count: int,
    total_spend: Decimal,
    errors: list[dict],
) -> None:
    """Publish a summary report to SNS."""
    period = "Monthly" if reset_type == "monthly" else "Daily"

    message_lines = [
        f"Bedrock Budget {period} Reset Summary",
        f"{'=' * 40}",
        f"Users reset: {reset_count}",
        f"Users unlocked (deny policy removed): {unlocked_count}",
        f"Total spend cleared: ${total_spend:.8f}" if total_spend < Decimal("0.01") else f"Total spend cleared: ${total_spend:.2f}",
    ]

    if errors:
        message_lines.append(f"\nErrors ({len(errors)}):")
        for err in errors[:10]:  # Cap at 10 in the message
            message_lines.append(f"  - {err['user_id']}: {err['error']}")
        if len(errors) > 10:
            message_lines.append(f"  ... and {len(errors) - 10} more")

    message = "\n".join(message_lines)

    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"Bedrock Budget {period} Reset Complete",
            Message=message,
        )
        logger.info("Published reset summary to SNS")
    except ClientError:
        logger.exception("Failed to publish reset summary to SNS")
