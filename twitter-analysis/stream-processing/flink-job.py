from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKinesisConsumer
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.descriptors import Schema, Json, Kinesis, FileSystem
from pyflink.common.serialization import JsonRowDeserializationSchema, SimpleStringSchema
from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy
import json

# Define the deserialization schema
kinesis_deserialization_schema = SimpleStringSchema()
aws_region = 'ap-southeast-2'
input_stream_name = "TwitterInputStream"

# Set up the execution environment using DataStream API
env = StreamExecutionEnvironment.get_execution_environment()
# env.set_runtime_mode(RuntimeExecutionMode.BATCH)
env.set_parallelism(1)
table_env = StreamTableEnvironment.create(env)

# Kinesis properties
kinesis_properties = {
    'aws.region': aws_region,  
    'aws.credentials.provider': 'AUTO',  # Automatically uses the credentials configured in your environment
    'flink.stream.initpos': 'LATEST'  # Start reading from the latest record
}

# Define the input Kinesis stream
kinesis_consumer = FlinkKinesisConsumer(
    input_stream_name,
    kinesis_deserialization_schema,
    kinesis_properties)

env.add_source(kinesis_consumer)

# Define the table schema
table_env.connect(Kinesis()
    .version("aws-kinesis-connector-flink")
    .stream(input_stream_name)
    .region(aws_region)
    .with_format(Json().fail_on_missing_field(False).schema(DataTypes.ROW([
        DataTypes.FIELD("Datetime", DataTypes.STRING()),
        DataTypes.FIELD("Tweet_Id", DataTypes.STRING()),
        DataTypes.FIELD("Text", DataTypes.STRING()),
        DataTypes.FIELD("Username", DataTypes.STRING()),
        DataTypes.FIELD("Permalink", DataTypes.STRING()),
        DataTypes.FIELD("User", DataTypes.STRING()),
        DataTypes.FIELD("Outlinks", DataTypes.STRING()),
        DataTypes.FIELD("CountLinks", DataTypes.STRING()),
        DataTypes.FIELD("ReplyCount", DataTypes.INT()),
        DataTypes.FIELD("RetweetCount", DataTypes.INT()),
        DataTypes.FIELD("LikeCount", DataTypes.INT()),
        DataTypes.FIELD("QuoteCount", DataTypes.INT()),
        DataTypes.FIELD("ConversationId", DataTypes.STRING()),
        DataTypes.FIELD("Language", DataTypes.STRING()),
        DataTypes.FIELD("Source", DataTypes.STRING()),
        DataTypes.FIELD("Media", DataTypes.STRING()),
        DataTypes.FIELD("QuotedTweet", DataTypes.STRING()),
        DataTypes.FIELD("MentionedUsers", DataTypes.STRING()),
        DataTypes.FIELD("hashtag", DataTypes.STRING()),
        DataTypes.FIELD("hastag_counts", DataTypes.INT())
    ])))
    .with_schema(Schema()
        .field("Datetime", DataTypes.STRING())
        .field("Tweet_Id", DataTypes.STRING())
        .field("Text", DataTypes.STRING())
        .field("Username", DataTypes.STRING())
        .field("Permalink", DataTypes.STRING())
        .field("User", DataTypes.STRING())
        .field("Outlinks", DataTypes.STRING())
        .field("CountLinks", DataTypes.STRING())
        .field("ReplyCount", DataTypes.INT())
        .field("RetweetCount", DataTypes.INT())
        .field("LikeCount", DataTypes.INT())
        .field("QuoteCount", DataTypes.INT())
        .field("ConversationId", DataTypes.STRING())
        .field("Language", DataTypes.STRING())
        .field("Source", DataTypes.STRING())
        .field("Media", DataTypes.STRING())
        .field("QuotedTweet", DataTypes.STRING())
        .field("MentionedUsers", DataTypes.STRING())
        .field("hashtag", DataTypes.STRING())
        .field("hastag_counts", DataTypes.INT()))
    .in_append_mode()
    .register_table_source("tweets"))

# Transform the data
tweets = table_env.from_path("tweets")

# Create ReactionCounts column
tweets = tweets.add_columns((tweets['ReplyCount'] + tweets['RetweetCount'] + tweets['LikeCount'] + tweets['QuoteCount']).alias('ReactionCounts'))

# Put all hashtags in lowercase
tweets = tweets.add_columns(tweets['hashtag'].lower().alias('hashtag_lower'))

# Calculate trending hashtags and users
trending_hashtags = tweets.group_by("hashtag_lower").select("hashtag_lower, count(1) as count").order_by("count.desc").limit(10)
trending_users = tweets.group_by("Username").select("Username, sum(ReactionCounts) as interactions").order_by("interactions.desc").limit(10)

# Define the output S3 sink
table_env.connect(FileSystem().path('s3://your-output-bucket/processed-data/')
    .with_format(Json())
    .with_schema(Schema()
        .field("Datetime", DataTypes.STRING())
        .field("Tweet_Id", DataTypes.STRING())
        .field("Text", DataTypes.STRING())
        .field("Username", DataTypes.STRING())
        .field("Permalink", DataTypes.STRING())
        .field("User", DataTypes.STRING())
        .field("Outlinks", DataTypes.STRING())
        .field("CountLinks", DataTypes.STRING())
        .field("ReplyCount", DataTypes.INT())
        .field("RetweetCount", DataTypes.INT())
        .field("LikeCount", DataTypes.INT())
        .field("QuoteCount", DataTypes.INT())
        .field("ConversationId", DataTypes.STRING())
        .field("Language", DataTypes.STRING())
        .field("Source", DataTypes.STRING())
        .field("Media", DataTypes.STRING())
        .field("QuotedTweet", DataTypes.STRING())
        .field("MentionedUsers", DataTypes.STRING())
        .field("hashtag_lower", DataTypes.STRING())
        .field("hastag_counts", DataTypes.INT())
        .field("ReactionCounts", DataTypes.INT()))
    .partitioned_by(["hour", "minute", "second"])
    .in_append_mode()
    .register_table_sink("s3_sink")

# Write the transformed data to S3
tweets.insert_into("s3_sink")

# Execute the Flink job
env.execute("Twitter Stream Processing")
