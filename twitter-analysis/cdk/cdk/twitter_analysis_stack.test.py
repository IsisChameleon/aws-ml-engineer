import unittest
import aws_cdk as cdk
from aws_cdk import assertions
from twitter_analysis_stack import TwitterAnalysisStack

class TestTwitterAnalysisStack(unittest.TestCase):

    def setUp(self):
        self.app = cdk.App()
        self.stack = TwitterAnalysisStack(self.app, "TwitterAnalysisStack")
        self.template = assertions.Template.from_stack(self.stack)

    def test_kinesis_data_stream_created(self):
        # Check if Kinesis Data Stream resource is created
        self.template.resource_count_is("AWS::Kinesis::Stream", 1)

    def test_kinesis_data_stream_name(self):
        # Check if Kinesis Data Stream has the correct stream name
        self.template.has_resource_properties("AWS::Kinesis::Stream", {
            "Name": "TwitterAnalysisInputStream"
        })

    def test_kinesis_data_stream_shard_count(self):
        # Check if Kinesis Data Stream has the correct shard count
        self.template.has_resource_properties("AWS::Kinesis::Stream", {
            "ShardCount": 1
        })

    def test_kinesis_data_stream_properties(self):
        # Check if Kinesis Data Stream has the correct properties
        self.template.has_resource_properties("AWS::Kinesis::Stream", {
            "Name": "TwitterAnalysisInputStream",
            "ShardCount": 1
        })

    def test_kinesis_data_stream_arn(self):
        # Check if Kinesis Data Stream has an ARN
        self.template.has_resource("AWS::Kinesis::Stream", {
            "Properties": {
                "Name": "TwitterAnalysisInputStream",
                "ShardCount": 1
            }
        })

if __name__ == '__main__':
    unittest.main()