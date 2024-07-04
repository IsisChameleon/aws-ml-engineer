from aws_cdk import (
    Duration,
    Stack,
    aws_kinesis as kinesis,
    aws_kinesisanalytics as kda,
    aws_iam as iam,
    aws_s3 as s3,
    aws_cloudwatch as cloudwatch,
    CfnParameter,
    CfnOutput
)
from constructs import Construct

class TwitterAnalysisStack(Stack):

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

               # Description
        self.template_options.description = "Inspired from (SO0124) - Streaming Data Solution for Amazon Kinesis (KPL -> KDS -> KDA). Version v1.9.1"

        # Metadata
        self.template_options.metadata = {
            "AWS::CloudFormation::Interface": {
                "ParameterGroups": [
                    # {
                    #     "Label": {"default": "Amazon Kinesis Producer Library (KPL) configuration"},
                    #     "Parameters": ["ProducerVpcId", "ProducerSubnetId", "ProducerAmiId"]
                    # },
                    {
                        "Label": {"default": "Amazon Kinesis Data Streams configuration"},
                        "Parameters": ["ShardCount", "RetentionHours", "EnableEnhancedMonitoring"]
                    },
                    {
                        "Label": {"default": "Amazon Kinesis Data Analytics configuration"},
                        "Parameters": ["LogLevel", "ApplicationSubnetIds", "ApplicationSecurityGroupIds"]
                    }
                ],
                "ParameterLabels": {
                    # "ProducerVpcId": {"default": "VPC where the KPL instance should be launched"},
                    # "ProducerSubnetId": {"default": "Subnet where the KPL instance should be launched (needs access to Kinesis - either via IGW or NAT)"},
                    # "ProducerAmiId": {"default": "Amazon Machine Image for the KPL instance"},
                    "ShardCount": {"default": "Number of open shards"},
                    "RetentionHours": {"default": "Data retention period (hours)"},
                    "EnableEnhancedMonitoring": {"default": "Enable enhanced (shard-level) metrics"},
                    "LogLevel": {"default": "Verbosity of the CloudWatch Logs for the studio"},
                    "ApplicationSubnetIds": {"default": "(Optional) Comma-separated list of subnet ids for VPC connectivity (if informed, requires security groups to be included as well)"},
                    "ApplicationSecurityGroupIds": {"default": "(Optional) Comma-separated list of security groups ids for VPC connectivity (if informed, requires subnets to be included as well)"}
                }
            },
            "cdk_nag": {
                "rules_to_suppress": [
                    {"reason": "IAM role requires more permissions", "id": "AwsSolutions-IAM5"},
                    {"reason": "EC2 does not need ASG", "id": "AwsSolutions-EC29"}
                ]
            }
        }

        # Parameters
        shard_count = CfnParameter(self, "ShardCount", type="Number", default=2, min_value=1, max_value=200)
        retention_hours = CfnParameter(self, "RetentionHours", type="Number", default=24, min_value=24, max_value=8760)
        enable_enhanced_monitoring = CfnParameter(self, "EnableEnhancedMonitoring", type="String", default="false", allowed_values=["true", "false"])
        # producer_vpc_id = CfnParameter(self, "ProducerVpcId", type="AWS::EC2::VPC::Id")
        # producer_subnet_id = CfnParameter(self, "ProducerSubnetId", type="AWS::EC2::Subnet::Id")
        # producer_ami_id = CfnParameter(self, "ProducerAmiId", type="AWS::SSM::Parameter::Value<AWS::EC2::Image::Id>", default="/aws/service/ami-amazon-linux-latest/amzn2-ami-hvm-x86_64-gp2")
        # log_level = CfnParameter(self, "LogLevel", type="String", default="INFO", allowed_values=["DEBUG", "ERROR", "INFO", "WARN"])
        # application_subnet_ids = CfnParameter(self, "ApplicationSubnetIds", type="CommaDelimitedList")
        # application_security_group_ids = CfnParameter(self, "ApplicationSecurityGroupIds", type="CommaDelimitedList")


        # Kinesis Data Stream
        kinesis_stream = kinesis.Stream(self, "KinesisDataStream",
                                     stream_name="TwitterAnalysisInputStream",
                                     retention_period=Duration.hours(retention_hours.value_as_number),
                                     shard_count=shard_count.value_as_number)

        # Create S3 Bucket
        s3_bucket = s3.Bucket(self, "TwitterAnalysisBucket")

        # IAM Role for Kinesis Data Analytics
        analytics_role = iam.Role(self, "KinesisAnalyticsRole",
                                  assumed_by=iam.ServicePrincipal("kinesisanalytics.amazonaws.com"),
                                  managed_policies=[
                                      iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonKinesisAnalyticsFullAccess"),
                                      iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonKinesisAnalyticsReadOnly")
                                  ])
        
        # IAM Role for Lambda and Kinesis Enhanced Monitoring
        # kds_role = iam.Role(self, "KdsRole44D602FE",
        #                     assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        #                     inline_policies={
        #                         "CloudWatchLogsPolicy": iam.PolicyDocument(
        #                             statements=[
        #                                 iam.PolicyStatement(
        #                                     actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
        #                                     effect=iam.Effect.ALLOW,
        #                                     resources=[f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws/lambda/*"]
        #                                 )
        #                             ]
        #                         ),
        #                         "MonitoringPolicy": iam.PolicyDocument(
        #                             statements=[
        #                                 iam.PolicyStatement(
        #                                     actions=["kinesis:EnableEnhancedMonitoring", "kinesis:DisableEnhancedMonitoring"],
        #                                     effect=iam.Effect.ALLOW,
        #                                     resources=["*"]
        #                                 )
        #                             ]
        #                         )
        #                     })

        # # Kinesis Data Analytics Application
        # analytics_application = kda.CfnApplication(self, "TwitterAnalysisApplication",
        #                                            inputs=[{
        #                                                "namePrefix": "exampleInput",
        #                                                "kinesisStreamsInput": {
        #                                                    "resourceArn": data_stream.stream_arn,
        #                                                    "roleArn": analytics_role.role_arn
        #                                                },
        #                                                "inputSchema": {
        #                                                    "recordFormat": {
        #                                                        "recordFormatType": "JSON",
        #                                                        "mappingParameters": {
        #                                                            "jsonMappingParameters": {
        #                                                                "recordRowPath": "$"
        #                                                            }
        #                                                        }
        #                                                    },
        #                                                    "recordColumns": [
        #                                                        {"name": "exampleColumn", "sqlType": "VARCHAR(16)", "mapping": "$.exampleColumn"}
        #                                                    ]
        #                                                }
        #                                            }])
        # Create Kinesis Data Analytics Application
        analytics_application = kda.CfnApplication(
            self, "TwitterStreamAnalytics",
            application_name="TwitterStreamAnalytics",
            inputs=[
                kda.CfnApplication.InputProperty(
                    name_prefix="TwitterStream",
                    kinesis_streams_input=kda.CfnApplication.KinesisStreamsInputProperty(
                        resource_arn=kinesis_stream.stream_arn,
                        role_arn=analytics_role.role_arn  
                    ),
                    input_schema=kda.CfnApplication.InputSchemaProperty(
                        record_columns=[
                            kda.CfnApplication.RecordColumnProperty(
                                name="Datetime",
                                sql_type="VARCHAR(256)",
                                mapping="$.Datetime"
                            ),
                            kda.CfnApplication.RecordColumnProperty(
                                name="TweetId",
                                sql_type="BIGINT",
                                mapping="$.TweetId"
                            ),
                            kda.CfnApplication.RecordColumnProperty(
                                name="Text",
                                sql_type="VARCHAR(256)",
                                mapping="$.Text"
                            ),
                            kda.CfnApplication.RecordColumnProperty(
                                name="Username",
                                sql_type="VARCHAR(256)",
                                mapping="$.Username"
                            ),
                            kda.CfnApplication.RecordColumnProperty(
                                name="Permalink",
                                sql_type="VARCHAR(256)",
                                mapping="$.Permalink"
                            ),
                            kda.CfnApplication.RecordColumnProperty(
                                name="User",
                                sql_type="VARCHAR(256)",
                                mapping="$.User"
                            ),
                            kda.CfnApplication.RecordColumnProperty(
                                name="Outlinks",
                                sql_type="VARCHAR(256)",
                                mapping="$.Outlinks"
                            ),
                            kda.CfnApplication.RecordColumnProperty(
                                name="CountLinks",
                                sql_type="VARCHAR(256)",
                                mapping="$.CountLinks"
                            ),
                            kda.CfnApplication.RecordColumnProperty(
                                name="ReplyCount",
                                sql_type="INTEGER",
                                mapping="$.ReplyCount"
                            ),
                            kda.CfnApplication.RecordColumnProperty(
                                name="RetweetCount",
                                sql_type="INTEGER",
                                mapping="$.RetweetCount"
                            ),
                            kda.CfnApplication.RecordColumnProperty(
                                name="LikeCount",
                                sql_type="INTEGER",
                                mapping="$.LikeCount"
                            ),
                            kda.CfnApplication.RecordColumnProperty(
                                name="QuoteCount",
                                sql_type="INTEGER",
                                mapping="$.QuoteCount"
                            ),
                            kda.CfnApplication.RecordColumnProperty(
                                name="ConversationId",
                                sql_type="BIGINT",
                                mapping="$.ConversationId"
                            ),
                            kda.CfnApplication.RecordColumnProperty(
                                name="Language",
                                sql_type="VARCHAR(256)",
                                mapping="$.Language"
                            ),
                            kda.CfnApplication.RecordColumnProperty(
                                name="Source",
                                sql_type="VARCHAR(256)",
                                mapping="$.Source"
                            ),
                            kda.CfnApplication.RecordColumnProperty(
                                name="Media",
                                sql_type="VARCHAR(256)",
                                mapping="$.Media"
                            ),
                            kda.CfnApplication.RecordColumnProperty(
                                name="QuotedTweet",
                                sql_type="VARCHAR(256)",
                                mapping="$.QuotedTweet"
                            ),
                            kda.CfnApplication.RecordColumnProperty(
                                name="MentionedUsers",
                                sql_type="VARCHAR(256)",
                                mapping="$.MentionedUsers"
                            ),
                            kda.CfnApplication.RecordColumnProperty(
                                name="hashtag",
                                sql_type="VARCHAR(256)",
                                mapping="$.hashtag"
                            ),
                            kda.CfnApplication.RecordColumnProperty(
                                name="hastag_counts",
                                sql_type="INTEGER",
                                mapping="$.hastag_counts"
                            ),
                        ],
                        record_format=kda.CfnApplication.RecordFormatProperty(
                            record_format_type="JSON"
                        )
                    )
                )
            ]
        )

        # Optionally add CloudWatch monitoring
        if self.node.try_get_context("enable_cloudwatch_monitoring"):
            alarm = cloudwatch.Alarm(self, "KinesisDataStreamAlarm",
                                     metric=kinesis_stream.metric("GetRecords.IteratorAgeMilliseconds"),
                                     threshold=1000,
                                     evaluation_periods=1,
                                     alarm_description="Alarm if the iterator age of the stream exceeds 1000ms")
