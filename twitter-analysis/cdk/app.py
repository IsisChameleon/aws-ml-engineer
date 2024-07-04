import os
import aws_cdk as cdk
from cdk.twitter_analysis_stack import TwitterAnalysisStack

app = cdk.App()
TwitterAnalysisStack(app, "TwitterAnalysisStack")

app.synth()