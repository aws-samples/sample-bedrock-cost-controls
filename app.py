#!/usr/bin/env python3
import aws_cdk as cdk

from infra.cost_controls_stack import BedrockCostControlsStack

app = cdk.App()

stack = BedrockCostControlsStack(
    app,
    "BedrockCostControlsStack",
    env=cdk.Environment(
        account=app.node.try_get_context("account"),
        region=app.node.try_get_context("region") or "us-east-1",
    ),
    description="Near real-time Bedrock per-user cost enforcement with IAM deny policies",
)

# Optional cdk-nag scan against the AWS Solutions ruleset:
#
#   cdk synth -c nag=true
#
# Opt-in rather than always-on for two reasons: cdk-nag is a dev-time dependency
# that deployers should not need installed, and a future cdk-nag release adding
# rules would otherwise turn into a failed deploy rather than a failed review.
if str(app.node.try_get_context("nag") or "").lower() == "true":
    import cdk_nag

    stack.apply_nag_suppressions()
    cdk.Aspects.of(app).add(cdk_nag.AwsSolutionsChecks(verbose=True))

app.synth()
