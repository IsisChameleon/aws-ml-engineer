import json
import logging
import math
import time

import boto3

logging.basicConfig(level=logging.INFO)

STREAM_NAME = "ExampleInputStream-RCF" # this is the name of your stream in AWSaws kinesis list-streams

def get_data(time):
    rad = (time/100)%360
    val = math.sin(rad)*10 + 10

    if rad > 2.4 and rad < 2.6:
        val = -17

    return {'time': time, 'value': val}

def generate(stream_name, kinesis_client):
    t = 0

    while True:
        data = get_data(t)
        try:
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(data),
                PartitionKey="partitionkey")
            logging.info(f"Successfully put record: {response}")
        except Exception as e:
            logging.error(f"Failed to put record: {e}")

        t += 1
        time.sleep(0.5)



if __name__ == '__main__':
    generate(STREAM_NAME, boto3.client('kinesis', region_name='us-west-2')) #<<< make sure to put the right region name