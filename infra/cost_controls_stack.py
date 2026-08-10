"""CDK stack for near real-time Bedrock cost enforcement."""

from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_logs_destinations as log_destinations,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_sqs as sqs,
)
from constructs import Construct


class BedrockCostControlsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ---------------------------------------------------------------
        # Context values (overridable at deploy time via -c key=value)
        # ---------------------------------------------------------------
        bedrock_log_group_name = self.node.try_get_context(
            "bedrock_log_group_name"
        ) or "/aws/bedrock/model-invocations"
        alert_email = self.node.try_get_context("alert_email") or ""
        default_daily_limit = self.node.try_get_context("default_daily_limit") or "50"
        default_monthly_limit = self.node.try_get_context("default_monthly_limit") or "500"
        reset_timezone = self.node.try_get_context("reset_timezone") or "America/Chicago"

        # When "true" (default) the enforcement Lambda refuses to attach a deny
        # policy it cannot scope to a single role session. On a shared role an
        # unscoped deny would block Bedrock for every principal assuming it.
        # Only set to "false" if every role in the account is single-user.
        require_scoped_deny = (
            self.node.try_get_context("require_scoped_deny") or "true"
        )

        # Fallback prompt-cache rates, as multipliers of the uncached input rate,
        # applied only to model families without a built-in ratio and only until
        # the Pricing Sync Lambda populates real rates from the Price List API.
        # Anthropic and Amazon Nova ratios are built into the handler.
        cache_read_multiplier = self.node.try_get_context("cache_read_multiplier") or "0.10"
        cache_write_multiplier = self.node.try_get_context("cache_write_multiplier") or "1.25"
        cache_write_1h_multiplier = (
            self.node.try_get_context("cache_write_1h_multiplier") or "2.00"
        )

        # Which Price List rate scope to store. "global" matches cross-region
        # inference profiles (us./eu./apac. prefixed model IDs), which is the common
        # pattern; "regional" matches bare model IDs pinned to one Region. Global
        # rates run roughly 10% below Regional.
        rate_scope = self.node.try_get_context("rate_scope") or "global"

        # ---------------------------------------------------------------
        # SNS Topic for budget alerts
        # ---------------------------------------------------------------
        # enforce_ssl adds a topic policy denying Publish over plaintext HTTP.
        # Server-side encryption is deliberately left off: it would require a
        # customer-managed KMS key, and these notifications carry a user ID and a
        # dollar amount rather than anything sensitive.
        self.alert_topic = sns.Topic(
            self,
            "BudgetAlertTopic",
            display_name="Bedrock Budget Alerts",
            enforce_ssl=True,
        )
        if alert_email:
            self.alert_topic.add_subscription(subs.EmailSubscription(alert_email))

        # ---------------------------------------------------------------
        # DynamoDB Tables
        # ---------------------------------------------------------------
        # Point-in-time recovery on the three durable tables. The usage table is
        # the important one: it holds the live spend accumulators that drive
        # enforcement, so losing it mid-period loses every user's budget state and
        # silently resets everyone to zero spent.
        pitr = dynamodb.PointInTimeRecoverySpecification(
            point_in_time_recovery_enabled=True
        )

        self.pricing_table = dynamodb.Table(
            self,
            "BedrockModelPricing",
            table_name="BedrockModelPricing",
            partition_key=dynamodb.Attribute(
                name="model_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery_specification=pitr,
        )

        self.usage_table = dynamodb.Table(
            self,
            "BedrockUserUsage",
            table_name="BedrockUserUsage",
            partition_key=dynamodb.Attribute(
                name="user_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery_specification=pitr,
        )

        self.budget_config_table = dynamodb.Table(
            self,
            "BedrockBudgetConfig",
            table_name="BedrockBudgetConfig",
            partition_key=dynamodb.Attribute(
                name="user_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery_specification=pitr,
        )

        # Deduplication table with TTL for at-least-once delivery handling
        self.dedup_table = dynamodb.Table(
            self,
            "BedrockInvocationDedup",
            table_name="BedrockInvocationDedup",
            partition_key=dynamodb.Attribute(
                name="request_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl",
        )

        # ---------------------------------------------------------------
        # Cost Enforcement Lambda
        # ---------------------------------------------------------------
        # Dead letter queue. CloudWatch Logs invokes the subscription-filter
        # destination ASYNCHRONOUSLY, so throttled or failing invocations are
        # retried and then discarded. Discarded events mean unmetered spend and
        # silent enforcement failure, which is the worst failure mode this
        # solution has. The DLQ makes those events recoverable and alarmable.
        self.enforcement_dlq = sqs.Queue(
            self,
            "CostEnforcementDLQ",
            queue_name="BedrockCostEnforcementDLQ",
            retention_period=Duration.days(14),
            enforce_ssl=True,
        )

        self.enforcement_lambda = lambda_.Function(
            self,
            "CostEnforcementLambda",
            function_name="BedrockCostEnforcement",
            runtime=lambda_.Runtime.PYTHON_3_14,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("lambdas/cost_enforcement"),
            timeout=Duration.seconds(60),
            memory_size=256,
            environment={
                "PRICING_TABLE": self.pricing_table.table_name,
                "USAGE_TABLE": self.usage_table.table_name,
                "BUDGET_CONFIG_TABLE": self.budget_config_table.table_name,
                "DEDUP_TABLE": self.dedup_table.table_name,
                "SNS_TOPIC_ARN": self.alert_topic.topic_arn,
                "DEFAULT_DAILY_LIMIT": default_daily_limit,
                "DEFAULT_MONTHLY_LIMIT": default_monthly_limit,
                "REQUIRE_SCOPED_DENY": require_scoped_deny,
                "CACHE_READ_MULTIPLIER": cache_read_multiplier,
                "CACHE_WRITE_MULTIPLIER": cache_write_multiplier,
                "CACHE_WRITE_1H_MULTIPLIER": cache_write_1h_multiplier,
                "METRIC_NAMESPACE": "BedrockCostControls",
            },
            reserved_concurrent_executions=50,
            dead_letter_queue=self.enforcement_dlq,
            retry_attempts=2,
        )

        # Grant DynamoDB permissions
        self.pricing_table.grant_read_data(self.enforcement_lambda)
        self.usage_table.grant_read_write_data(self.enforcement_lambda)
        self.budget_config_table.grant_read_data(self.enforcement_lambda)
        self.dedup_table.grant_read_write_data(self.enforcement_lambda)

        # Grant SNS publish
        self.alert_topic.grant_publish(self.enforcement_lambda)

        # Grant IAM permissions to attach/detach inline policies on any role or user.
        #
        # iam:GetRole is required to read the role's unique ID (AROA...), which is
        # needed to build the `aws:userid` condition that scopes the deny to a
        # single role session. Without it the Lambda cannot produce a scoped policy
        # and (with REQUIRE_SCOPED_DENY=true) will decline to enforce rather than
        # deny every user of a shared role.
        # Scoped to this account. The principal being denied is not known until
        # runtime, so the role/user path must stay wildcarded, but there is never
        # a reason to write an inline policy into another account.
        self.enforcement_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "iam:GetRole",
                    "iam:PutRolePolicy",
                    "iam:DeleteRolePolicy",
                    "iam:GetRolePolicy",
                    "iam:PutUserPolicy",
                    "iam:DeleteUserPolicy",
                    "iam:GetUserPolicy",
                ],
                resources=[
                    f"arn:{self.partition}:iam::{self.account}:role/*",
                    f"arn:{self.partition}:iam::{self.account}:user/*",
                ],
            )
        )

        # ---------------------------------------------------------------
        # CloudWatch Logs Subscription Filter
        # ---------------------------------------------------------------
        bedrock_log_group = logs.LogGroup.from_log_group_name(
            self,
            "BedrockLogGroup",
            bedrock_log_group_name,
        )

        logs.SubscriptionFilter(
            self,
            "BedrockInvocationFilter",
            log_group=bedrock_log_group,
            destination=log_destinations.LambdaDestination(self.enforcement_lambda),
            filter_pattern=logs.FilterPattern.all_events(),
        )

        # ---------------------------------------------------------------
        # Alarms — make the silent failure modes visible
        # ---------------------------------------------------------------
        alarm_action = cw_actions.SnsAction(self.alert_topic)

        def _add_alarm(alarm_id: str, alarm_name: str, metric, threshold: int,
                       description: str, evaluation_periods: int = 1) -> None:
            alarm = cloudwatch.Alarm(
                self,
                alarm_id,
                alarm_name=alarm_name,
                metric=metric,
                threshold=threshold,
                evaluation_periods=evaluation_periods,
                comparison_operator=(
                    cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD
                ),
                treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
                alarm_description=description,
            )
            alarm.add_alarm_action(alarm_action)

        # Throttling is the highest-severity signal: reserved concurrency is 50,
        # and throttled async invocations are eventually discarded, so spend goes
        # unmetered and users can exceed budget undetected.
        _add_alarm(
            "EnforcementThrottleAlarm",
            "BedrockCostControls-EnforcementThrottled",
            self.enforcement_lambda.metric_throttles(period=Duration.minutes(5)),
            threshold=1,
            description=(
                "Cost enforcement Lambda is being throttled. Invocation log events "
                "may be discarded, causing unmetered Bedrock spend and enforcement "
                "gaps. Raise reserved_concurrent_executions."
            ),
        )

        _add_alarm(
            "EnforcementErrorAlarm",
            "BedrockCostControls-EnforcementErrors",
            self.enforcement_lambda.metric_errors(period=Duration.minutes(5)),
            threshold=1,
            description=(
                "Cost enforcement Lambda is erroring. Spend may not be tracked."
            ),
        )

        # Anything landing in the DLQ is spend that was never metered.
        _add_alarm(
            "EnforcementDlqAlarm",
            "BedrockCostControls-EnforcementDLQNotEmpty",
            self.enforcement_dlq.metric_approximate_number_of_messages_visible(
                period=Duration.minutes(5)
            ),
            threshold=1,
            description=(
                "Invocation log events failed processing and landed in the DLQ. "
                "These represent Bedrock spend that was never attributed."
            ),
        )

        # A caching-capable request metered from the log envelope alone is a known
        # undercount, because inputTokenCount excludes cache read/write tokens.
        _add_alarm(
            "EnvelopeOnlyMeteringAlarm",
            "BedrockCostControls-EnvelopeOnlyMetering",
            cloudwatch.Metric(
                namespace="BedrockCostControls",
                metric_name="EnvelopeOnlyMetering",
                statistic="Sum",
                period=Duration.minutes(15),
            ),
            threshold=100,
            description=(
                "Invocations are being metered from the log envelope only, without "
                "the usage object. Prompt cache read/write tokens are invisible for "
                "these records, so cached workloads are undercounted. Check whether "
                "model invocation logging has text data delivery enabled."
            ),
        )

        # Budget exceeded but enforcement declined because the deny could not be
        # scoped to a single session. Requires an operator to intervene.
        _add_alarm(
            "UnscopableDenyAlarm",
            "BedrockCostControls-UnscopableDenySkipped",
            cloudwatch.Metric(
                namespace="BedrockCostControls",
                metric_name="UnscopableDenySkipped",
                statistic="Sum",
                period=Duration.minutes(5),
            ),
            threshold=1,
            description=(
                "A user exceeded budget but no deny policy was attached because it "
                "could not be scoped to their role session. Verify the Lambda has "
                "iam:GetRole, then enforce manually if appropriate."
            ),
        )

        _add_alarm(
            "DenyAttachFailureAlarm",
            "BedrockCostControls-DenyAttachFailures",
            cloudwatch.Metric(
                namespace="BedrockCostControls",
                metric_name="DenyAttachFailures",
                statistic="Sum",
                period=Duration.minutes(5),
            ),
            threshold=1,
            description=(
                "Failed to attach an IAM deny policy for a user over budget. Likely "
                "IAM throttling or a missing permission. The user is still able to "
                "invoke Bedrock."
            ),
        )

        # ---------------------------------------------------------------
        # Budget Reset Lambda
        # ---------------------------------------------------------------
        self.reset_lambda = lambda_.Function(
            self,
            "BudgetResetLambda",
            function_name="BedrockBudgetReset",
            runtime=lambda_.Runtime.PYTHON_3_14,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("lambdas/budget_reset"),
            # The reset performs a filtered Scan, which reads every item in the
            # usage table before applying the filter. 300s is comfortable into the
            # low tens of thousands of users; beyond that, move to a sparse GSI or
            # a segmented parallel scan.
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "USAGE_TABLE": self.usage_table.table_name,
                "SNS_TOPIC_ARN": self.alert_topic.topic_arn,
                "RESET_TIMEZONE": reset_timezone,
            },
        )

        self.usage_table.grant_read_write_data(self.reset_lambda)
        self.alert_topic.grant_publish(self.reset_lambda)

        # IAM permissions for removing deny policies (roles and users), scoped to
        # this account for the same reason as the enforcement Lambda above.
        self.reset_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "iam:DeleteRolePolicy",
                    "iam:GetRolePolicy",
                    "iam:DeleteUserPolicy",
                    "iam:GetUserPolicy",
                ],
                resources=[
                    f"arn:{self.partition}:iam::{self.account}:role/*",
                    f"arn:{self.partition}:iam::{self.account}:user/*",
                ],
            )
        )

        # ---------------------------------------------------------------
        # EventBridge Schedules for daily + monthly reset
        # ---------------------------------------------------------------
        # Daily reset at midnight CT (06:00 UTC for CDT, 05:00 UTC for CST)
        daily_rule = events.Rule(
            self,
            "DailyResetRule",
            rule_name="BedrockBudgetDailyReset",
            schedule=events.Schedule.cron(minute="0", hour="6", day="*", month="*"),
            description="Reset daily Bedrock spend accumulators at midnight CT",
        )
        daily_rule.add_target(
            targets.LambdaFunction(
                self.reset_lambda,
                event=events.RuleTargetInput.from_object({"reset_type": "daily"}),
            )
        )

        # Monthly reset on 1st of each month at midnight CT
        monthly_rule = events.Rule(
            self,
            "MonthlyResetRule",
            rule_name="BedrockBudgetMonthlyReset",
            schedule=events.Schedule.cron(minute="0", hour="6", day="1", month="*"),
            description="Reset monthly Bedrock spend accumulators on 1st of month",
        )
        monthly_rule.add_target(
            targets.LambdaFunction(
                self.reset_lambda,
                event=events.RuleTargetInput.from_object({"reset_type": "monthly"}),
            )
        )

        # ---------------------------------------------------------------
        # Pricing Sync Lambda — keeps model pricing up to date
        # ---------------------------------------------------------------
        self.pricing_sync_lambda = lambda_.Function(
            self,
            "PricingSyncLambda",
            function_name="BedrockPricingSync",
            runtime=lambda_.Runtime.PYTHON_3_14,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("lambdas/pricing_sync"),
            timeout=Duration.seconds(300),
            memory_size=512,
            environment={
                "PRICING_TABLE": self.pricing_table.table_name,
                "RATE_SCOPE": rate_scope,
            },
        )

        # Grant DynamoDB read/write for pricing table
        self.pricing_table.grant_read_write_data(self.pricing_sync_lambda)

        # Grant Bedrock ListFoundationModels
        self.pricing_sync_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["bedrock:ListFoundationModels"],
                resources=["*"],
            )
        )

        # Grant Price List API access (read-only)
        self.pricing_sync_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["pricing:GetProducts"],
                resources=["*"],
            )
        )

        # Schedule: run daily at 2 AM UTC (gives time before business hours)
        pricing_sync_rule = events.Rule(
            self,
            "PricingSyncRule",
            rule_name="BedrockPricingSyncDaily",
            schedule=events.Schedule.cron(minute="0", hour="2", day="*", month="*"),
            description="Sync Bedrock model pricing from AWS Price List API daily",
        )
        pricing_sync_rule.add_target(
            targets.LambdaFunction(self.pricing_sync_lambda)
        )

    # -------------------------------------------------------------------
    # cdk-nag
    # -------------------------------------------------------------------
    def apply_nag_suppressions(self) -> None:
        """Record justified suppressions for the AwsSolutions rule pack.

        cdk-nag is imported inside this method rather than at module scope so it
        stays a development-only dependency; deploying the stack must not require
        it. app.py calls this only when the scan is enabled with -c nag=true.

        Every remaining finding is suppressed with a stated reason. Nothing here
        is suppressed because it was inconvenient to fix -- the fixable findings
        (account-scoped IAM resources, SNS SSL enforcement, point-in-time
        recovery on the durable tables, current Lambda runtime) were fixed.
        """
        from cdk_nag import NagPackSuppression, NagSuppressions

        # Reason text is bound to a name rather than written inline. A long
        # implicitly concatenated string sitting inside a list literal reads
        # ambiguously -- it is easy to mistake the fragments for separate list
        # elements -- and static analysis flags the pattern for that reason.
        dedup_pitr_reason = (
            "The deduplication table holds request IDs with a short TTL purely to "
            "make at-least-once log delivery idempotent. Every item is expected to "
            "expire, and a restored copy would carry no useful state. Point-in-time "
            "recovery is enabled on the three tables that do hold durable state."
        )
        basic_execution_reason = (
            "AWSLambdaBasicExecutionRole is attached automatically by the CDK "
            "Function construct and grants only CreateLogGroup, CreateLogStream and "
            "PutLogEvents. A customer managed equivalent would restate the same "
            "permissions without narrowing them."
        )
        runtime_principal_reason = (
            "Attaching and removing a scoped inline deny policy on a principal that "
            "is only identified at runtime is the core function of this solution. "
            "Scoped to this account and partition, and limited to inline role/user "
            "policy actions."
        )
        pricing_wildcard_reason = (
            "pricing:GetProducts and bedrock:ListFoundationModels do not support "
            "resource-level permissions, so a wildcard resource is the only valid "
            "value. Both are read-only and neither exposes customer data."
        )

        basic_execution_policy = (
            "Policy::arn:<AWS::Partition>:iam::aws:policy/"
            "service-role/AWSLambdaBasicExecutionRole"
        )
        account_scoped_principals = [
            "Resource::arn:<AWS::Partition>:iam::<AWS::AccountId>:role/*",
            "Resource::arn:<AWS::Partition>:iam::<AWS::AccountId>:user/*",
        ]

        NagSuppressions.add_resource_suppressions(
            self.dedup_table,
            [
                NagPackSuppression(
                    id="AwsSolutions-DDB3",
                    reason=dedup_pitr_reason,
                )
            ],
        )

        for fn in (
            self.enforcement_lambda,
            self.reset_lambda,
            self.pricing_sync_lambda,
        ):
            NagSuppressions.add_resource_suppressions(
                fn,
                [
                    NagPackSuppression(
                        id="AwsSolutions-IAM4",
                        reason=basic_execution_reason,
                        applies_to=[basic_execution_policy],
                    )
                ],
                apply_to_children=True,
            )

        # The identity to deny is discovered from an invocation log record at
        # runtime, so the principal path cannot be enumerated at deploy time. The
        # account and partition are pinned, and the action list is limited to
        # inline-policy operations -- it cannot create principals, attach managed
        # policies, or alter trust relationships.
        for fn in (self.enforcement_lambda, self.reset_lambda):
            NagSuppressions.add_resource_suppressions(
                fn,
                [
                    NagPackSuppression(
                        id="AwsSolutions-IAM5",
                        reason=runtime_principal_reason,
                        applies_to=account_scoped_principals,
                    )
                ],
                apply_to_children=True,
            )

        NagSuppressions.add_resource_suppressions(
            self.pricing_sync_lambda,
            [
                NagPackSuppression(
                    id="AwsSolutions-IAM5",
                    reason=pricing_wildcard_reason,
                    applies_to=["Resource::*"],
                )
            ],
            apply_to_children=True,
        )
