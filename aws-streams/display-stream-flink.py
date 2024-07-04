import json

import boto3
import matplotlib.pyplot as plt
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKinesisConsumer
from pyflink.table import DataTypes, StreamTableEnvironment, Table

# Initialize the StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
t_env = StreamTableEnvironment.create(env)

# Configure AWS credentials and Kinesis client
aws_region = 'us-west-2'
stream_name = 'ExampleInputStream-RCF'
# aws_access_key_id = 'your_access_key_id'
# aws_secret_access_key = 'your_secret_access_key'

kinesis_client = boto3.client(
    'kinesis',
    region_name=aws_region
    # aws_access_key_id=aws_access_key_id,
    # aws_secret_access_key=aws_secret_access_key
)

def get_kinesis_records(stream_name, kinesis_client):
    shard_id = kinesis_client.describe_stream(StreamName=stream_name)['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = kinesis_client.get_shard_iterator(StreamName=stream_name, ShardId=shard_id, ShardIteratorType='LATEST')['ShardIterator']
    
    while True:
        response = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=1000)
        shard_iterator = response['NextShardIterator']
        records = response['Records']
        for record in records:
            yield json.loads(record['Data'])
        if not records:
            break

# Define the custom source function
# Define the properties for the Kinesis consumer
kinesis_consumer_props = {
    'aws.region': aws_region,
    'aws.stream': stream_name,
    'flink.stream.initpos': 'LATEST'
}

# Create the Kinesis consumer
kinesis_consumer = FlinkKinesisConsumer(
    stream_name,
    SimpleStringSchema(),
    kinesis_consumer_props
)

# Add the source to the environment
data_stream = env.add_source(kinesis_consumer, Types.STRING())

# Parse the JSON data
def parse_json(record):
    return record['time'], record['value']

parsed_stream = data_stream.map(parse_json, output_type=DataTypes.ROW([DataTypes.LONG(), DataTypes.DOUBLE()]))

# Convert DataStream to Table
t_env.create_temporary_view('kinesis_table', parsed_stream, schema=DataTypes.ROW([
    DataTypes.FIELD("time", DataTypes.BIGINT()),
    DataTypes.FIELD("value", DataTypes.DOUBLE())
]))

# Query the table
result_table: Table = t_env.sql_query("SELECT time, value FROM kinesis_table")

# Convert the Table to a Pandas DataFrame
df = t_env.to_pandas(result_table)

# Plot the data
plt.figure(figsize=(10, 6))
plt.plot(df['time'], df['value'], marker='o', linestyle='-')
plt.title('Sinewave with Anomaly')
plt.xlabel('Time')
plt.ylabel('Value')
plt.grid(True)
plt.show()
