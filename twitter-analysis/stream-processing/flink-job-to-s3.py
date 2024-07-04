# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
# -*- coding: utf-8 -*-

"""
flink-job-to-s3.py
~~~~~~~~~~~~~~~~~~~
This module:
    1. Creates a table environment
    2. Creates a source table from a Kinesis Data Stream
    3. Creates a sink table writing to an S3 Bucket
    4. Queries from the Source Table and
       creates a tumbling window over 1 minute to calculate the average price over the window.
    5. These tumbling window results are inserted into the Sink table (S3)
"""

import json
import os
import re

from pyflink.table import (
    DataTypes,
    EnvironmentSettings,
    TableEnvironment,
    TableFunction,
)
from pyflink.table.expressions import call, col, lit
from pyflink.table.udf import udf, udtf
from pyflink.table.window import Tumble

# 1. Creates a Table Environment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

APPLICATION_PROPERTIES_FILE_PATH = "/etc/flink/application_properties.json"  # on kda

is_local = (
    True if os.environ.get("IS_LOCAL") else False
)  # set this env var in your local environment

if is_local:
    # only for local, overwrite variable to properties and pass in your jars delimited by a semicolon (;)
    APPLICATION_PROPERTIES_FILE_PATH = "twitter-analysis/stream-processing/application_properties.json"  # local

    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    table_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        "file:///"
        + CURRENT_DIR
        + "/../lib/flink-sql-connector-kinesis-4.3.0-1.19.jar"
    )

    table_env.get_config().get_configuration().set_string(
        "execution.checkpointing.mode", "EXACTLY_ONCE"
    )
    table_env.get_config().get_configuration().set_string(
        "execution.checkpointing.interval", "1min"
    )


def get_application_properties():
    if os.path.isfile(APPLICATION_PROPERTIES_FILE_PATH):
        with open(APPLICATION_PROPERTIES_FILE_PATH, "r") as file:
            contents = file.read()
            properties = json.loads(contents)
            return properties
    else:
        print('A file at "{}" was not found'.format(APPLICATION_PROPERTIES_FILE_PATH))
        print("Current directory content:")
        for item in os.listdir(os.getcwd()):
            print(item)


def property_map(props, property_group_id):
    for prop in props:
        if prop["PropertyGroupId"] == property_group_id:
            return prop["PropertyMap"]


# def create_source_table(table_name, stream_name, region, stream_initpos):
#     return """ CREATE TABLE {0} (
#                 ticker VARCHAR(6),
#                 price DOUBLE,
#                 event_time TIMESTAMP(3),
#                 WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
#               )
#               PARTITIONED BY (ticker)
#               WITH (
#                 'connector' = 'kinesis',
#                 'stream' = '{1}',
#                 'aws.region' = '{2}',
#                 'scan.stream.initpos' = '{3}',
#                 'format' = 'json',
#                 'json.timestamp-format.standard' = 'ISO-8601'
#               ) """.format(table_name, stream_name, region, stream_initpos)

def create_source_table(table_name, stream_name, region, stream_initpos):
    return """ CREATE TABLE {0} (
                Datetime STRING,
                Tweet_Id STRING,
                Text STRING,
                Username STRING,
                Permalink STRING,
                `User` STRING,
                Outlinks STRING,
                CountLinks STRING,
                ReplyCount INT,
                RetweetCount INT,
                LikeCount INT,
                QuoteCount INT,
                ConversationId STRING,
                `Language` STRING,
                Source STRING,
                Media STRING,
                QuotedTweet STRING,
                MentionedUsers STRING,
                hashtag STRING,
                hastag_counts INT,
                event_time TIMESTAMP(3),
                WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
              )
              WITH (
                'connector' = 'kinesis',
                'stream' = '{1}',
                'aws.region' = '{2}',
                'scan.stream.initpos' = '{3}',
                'format' = 'json',
                'json.timestamp-format.standard' = 'ISO-8601'
              ) """.format(table_name, stream_name, region, stream_initpos)


# def create_sink_table(table_name, bucket_name):
#     return """ CREATE TABLE {0} (
#                 ticker VARCHAR(6),
#                 price DOUBLE,
#                 event_time VARCHAR(64)
#               )
#               PARTITIONED BY (ticker)
#               WITH (
#                   'connector'='filesystem',
#                   'path'='s3a://{1}/',
#                   'format'='json',
#                   'sink.partition-commit.policy.kind'='success-file',
#                   'sink.partition-commit.delay' = '1 min'
#               ) """.format(table_name, bucket_name)

def create_sink_table(table_name, bucket_name):
    return """ CREATE TABLE {0} (
                Username STRING,
                ReactionCount INT,
                event_time TIMESTAMP(3)
              )
              PARTITIONED BY (Username, event_time)
              WITH (
                  'connector'='filesystem',
                  'path'='s3a://{1}/',
                  'format'='json',
                  'sink.partition-commit.policy.kind'='success-file',
                  'sink.partition-commit.delay' = '1 min'
              ) """.format(table_name, bucket_name)


# def create_sink_table(table_name, bucket_name):
#     return """ CREATE TABLE {0} (
#                 Datetime STRING,
#                 Tweet_Id STRING,
#                 Text STRING,
#                 Username STRING,
#                 Permalink STRING,
#                 `User` STRING,
#                 Outlinks STRING,
#                 CountLinks STRING,
#                 ReplyCount INT,
#                 RetweetCount INT,
#                 LikeCount INT,
#                 QuoteCount INT,
#                 ConversationId STRING,
#                 `Language` STRING,
#                 Source STRING,
#                 Media STRING,
#                 QuotedTweet STRING,
#                 MentionedUsers STRING,
#                 hashtag STRING,
#                 hastag_counts INT,
#                 event_time VARCHAR(64)
#               )
#               PARTITIONED BY (Datetime)
#               WITH (
#                   'connector'='filesystem',
#                   'path'='s3a://{1}/',
#                   'format'='json',
#                   'sink.partition-commit.policy.kind'='success-file',
#                   'sink.partition-commit.delay' = '1 min'
#               ) """.format(table_name, bucket_name)


# def perform_tumbling_window_aggregation(input_table_name):
#     # use SQL Table in the Table API
#     input_table = table_env.from_path(input_table_name)

#     # tumbling_window_table = (
#     #     input_table.window(
#     #         Tumble.over("1.minute").on("event_time").alias("one_minute_window")
#     #     )
#     #     .group_by("ticker, one_minute_window")
#     #     .select("ticker, price.avg as price, to_string(one_minute_window.end) as event_time")
#     # )

#     tumbling_window_table = (
#         input_table.window(
#             Tumble.over("1.minute").on("event_time").alias("one_minute_window")
#         )
#         .select("count(Tweet_Id), to_string(one_minute_window.end) as event_time")
#     )

#     return tumbling_window_table


def perform_tumbling_window_aggregation(input_table_name):
    # use SQL Table in the Table API
    input_table = table_env.from_path(input_table_name)

    # Define a tumbling window of 1 minute
    one_minute_duration = lit(1).minutes

    tumbling_window_table = (
        input_table.window(
            Tumble.over(one_minute_duration).on(col("event_time")).alias("one_minute_window")
        )
        .group_by(col("one_minute_window"))
        .select(col('Tweet_Id').count.alias('TweetCount'),
               col('one_minute_window').end.cast(DataTypes.TIMESTAMP(3)).alias('event_time'))
        # .select("count(Tweet_Id), to_string(one_minute_window.end) as event_time")
    )

    return tumbling_window_table


@udf(input_types=[DataTypes.TIMESTAMP(3)], result_type=DataTypes.STRING())
def to_string(i):
    return str(i)

table_env.create_temporary_system_function("to_string", to_string)

# UDF to clean hashtags
@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.ARRAY(DataTypes.STRING()))
def clean_hashtags(hashtag):
    if hashtag is None:
        return []
    cleaned = re.sub(r'[\[\]#\'"]', '', hashtag.lower())
    return cleaned.split(',')

class ExplodeHashtags(TableFunction):
    def eval(self, hashtags):
        if hashtags is not None:
            for hashtag in hashtags:
                yield hashtag


def perform_simple_transformation(input_table_name):
    # Register the UDF
    table_env.create_temporary_system_function("clean_hashtags", clean_hashtags)
    # Register the UDTF
    table_env.create_temporary_system_function("explode_hashtags", udtf(ExplodeHashtags(), result_types=[DataTypes.STRING()]))

    # Read from the source table
    source_table = table_env.from_path(input_table_name)

    # Perform transformations
    transformed_table = source_table \
        .add_columns(clean_hashtags(col('hashtag')).alias('hashtags')) \
        .add_columns((col('ReplyCount') + col('RetweetCount') + col('LikeCount') + col('QuoteCount')).alias('ReactionCount'))
    
    # Define a tumbling window of 1 hour
    tumble_window = Tumble.over(lit(1).hour).on(col('event_time')).alias('w')

    # Find the user with the most reactions within the window
    trending_users = transformed_table \
        .window(tumble_window) \
        .group_by(col('w'), col('Username')) \
        .select(col('Username'), col('ReactionCount').sum.alias('TotalReactions'), col('w').end.alias('window_end'))

    # Explode hashtags and find the top 5 trending hashtags within the window
    exploded_hashtags = transformed_table \
        .join_lateral(call('explode_hashtags', col('hashtags')).alias('hashtag')) \
        .select(col('event_time'), col('hashtag'))

    trending_hashtags = exploded_hashtags \
        .window(tumble_window) \
        .group_by(col('w'), col('hashtag')) \
        .select(col('hashtag'), col('hashtag').count.alias('hashtag_counts'), col('w').end.alias('window_end'))

    return trending_users, trending_hashtags



def perform_transformation(input_table_name):
    # Register the UDF
    table_env.create_temporary_system_function("clean_hashtags", clean_hashtags)
    # Register the UDTF
    table_env.create_temporary_system_function("explode_hashtags", udtf(ExplodeHashtags(), result_types=[DataTypes.STRING()]))

    # Read from the source table
    source_table = table_env.from_path(input_table_name)

    # Perform transformations
    transformed_table = source_table \
        .add_columns(clean_hashtags(col('hashtag')).alias('hashtags')) \
        .add_columns((col('ReplyCount') + col('RetweetCount') + col('LikeCount') + col('QuoteCount')).alias('ReactionCount'))
    
        # Define a tumbling window of 1 hour
    tumble_window = Tumble.over(lit(1).hour).on(col('event_time')).alias('w')

    # Find the user with the most reactions within the window
   
    trending_users = transformed_table \
        .window(tumble_window) \
        .group_by(col('w'), col('Username')) \
        .select(col('Username'), col('ReactionCount').sum.alias('TotalReactions'), col('w').end.cast(DataTypes.TIMESTAMP(3)).alias('window_end')) \
        .order_by(col('window_end'), col('TotalReactions').desc) \
        .fetch(3)

    # Explode hashtags and find the top 5 trending hashtags within the window
    exploded_hashtags = transformed_table \
        .join_lateral(call('explode_hashtags', col('hashtags')).alias('exploded_hashtags')) \
        .select(col('event_time'), col('exploded_hashtags').alias('hashtag'))

    # trending_hashtags = exploded_hashtags \
    #     .window(tumble_window) \
    #     .group_by(col('w'), col('hashtag')) \
    #     .select(col('hashtag'), col('hashtag').count.alias('hashtag_counts'), col('w').end.alias('window_end')) \
    #     .order_by(col('window_end'), col('hashtag_counts').desc) \
    #     .fetch(5)
    
    trending_hashtags = exploded_hashtags \
        .window(tumble_window) \
        .group_by(col('w'), col('hashtag')) \
        .select(col('hashtag'), col('hashtag').count.alias('hashtag_counts'), col('w').end.alias('window_end')) \
        .order_by(col('window_end'), col('hashtag_counts').desc) \
        .limit(5)

    return trending_users, trending_hashtags

def main():
    # Application Property Keys
    input_property_group_key = "consumer.config.0"
    sink_property_group_key = "sink.config.0" 

    input_stream_key = "input.stream.name"
    input_region_key = "aws.region"
    input_starting_position_key = "scan.stream.initpos"

    output_sink_key = "output.bucket.name"

    # tables
    input_table_name = "input_table"
    output_table_name = "output_table"

    # get application properties
    props = get_application_properties()

    input_property_map = property_map(props, input_property_group_key)
    output_property_map = property_map(props, sink_property_group_key)

    input_stream = input_property_map[input_stream_key]
    input_region = input_property_map[input_region_key]
    stream_initpos = input_property_map[input_starting_position_key]

    output_bucket_name = output_property_map[output_sink_key]

    # 2. Creates a source table from a Kinesis Data Stream
    create_source = create_source_table(input_table_name, input_stream, input_region, stream_initpos)
    table_env.execute_sql(create_source)

    # 3. Creates a sink table writing to an S3 Bucket
    create_sink = create_sink_table(output_table_name, output_bucket_name)
    table_env.execute_sql(create_sink)

    # 4. Transform the source table
    # - This transformation is done using the Table API
    # - new column hashtags needs to be an array of strings using the following transformation: df['hashtags'] = df['hashtag'].apply(lambda x: re.sub(r'[\[\]#\'"]', '', x.lower()))
    # - new column ReactionCount : df['ReactionCount'] = df['ReplyCount'] + df['RetweetCount'] + df['LikeCount'] + df['QuoteCount']
    # Determine user with the most ReactionCount from any tweets and its top tweet
    trending_users, trending_hashtags = perform_transformation(input_table_name)
    
    # Register the transformed tables as views for further processing or output
    # table_env.create_temporary_view('TransformedTable', transformed_table)
    table_env.create_temporary_view('TrendingUsers', trending_users)
    table_env.create_temporary_view('TrendingHashtags', trending_hashtags)

    # 5. Write to the sink table (S3)
    #trending_users.execute_insert("output_table")
    # trending_hashtags.execute_insert("output_table")
    
    table_result = table_env.execute_sql(f"INSERT INTO {output_table_name} SELECT * FROM TrendingUsers")

    if is_local:
        table_result.wait()
    else:
        # get job status through TableResult
        print(table_result.get_job_client().get_job_status())


if __name__ == "__main__":
    main()