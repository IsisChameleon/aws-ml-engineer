import csv
import json
import random
import time

import boto3

"""
This is our data producer that will simulate sending real time data to Kinesis Data Stream.
"""

kinesis_client = boto3.client('kinesis', region_name='ap-southeast-2')
DEBUG=True

def send_to_kinesis(stream_name, data):    
    if DEBUG:
        print(f'Put record to Kinesis: StreamName {stream_name} Data {json.dumps(data)}')
        return
    
    response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(data),
        PartitionKey="partition_key"
    )

    # If the message was not sucssfully sent print an error message
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        print('Error!')
        print(response)

def read_and_send_data(file_path, stream_name):
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            send_to_kinesis(stream_name, row)
            time.sleep(random.uniform(0.5, 2.0))  # Random delay to simulate real-time

if __name__ == "__main__":
    file_path = '/workspaces/data/chatgpt1.csv/chatgpt1.csv'
    stream_name = 'twitter-analysis'
    read_and_send_data(file_path, stream_name)
