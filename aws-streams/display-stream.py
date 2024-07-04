import time

import boto3

# Create a Kinesis client
kinesis = boto3.client('kinesis', region_name='us-west-2')

# Specify the stream name
stream_name = 'ExampleInputStream-RCF'

# Describe the stream to get its details
response = kinesis.describe_stream(StreamName=stream_name)
stream_details = response['StreamDescription']

# Get the latest shard iterator
shard_id = stream_details['Shards'][0]['ShardId']
shard_iterator = kinesis.get_shard_iterator(
    StreamName=stream_name,
    ShardId=shard_id,
    ShardIteratorType='LATEST'
)['ShardIterator']

# Retrieve and display the data records
while True:
    records_response = kinesis.get_records(ShardIterator=shard_iterator, Limit=10)
    records = records_response['Records']
    if records:
        for record in records:
            print(f"Data: {record['Data'].decode('utf-8')}")
    shard_iterator = records_response['NextShardIterator']

    # Add a delay to avoid exceeding the read throughput limits
    time.sleep(1)
