import boto3

s3 = boto3.client('s3')

response = s3.select_object_content(
    Bucket='aws-ml-engineer-001',
    Key='twitter-analysis/initial-dataset/chatgpt1.csv',
    ExpressionType='SQL',
    Expression="SELECT * FROM S3Object WHERE Username = 'GRDecter' LIMIT 5",
    InputSerialization={
        'CSV': {
            'FileHeaderInfo': 'USE',
            'RecordDelimiter': '\n',
            'FieldDelimiter': ',',
            'QuoteCharacter': '"',
            'AllowQuotedRecordDelimiter': True
        }
    },
    OutputSerialization={'CSV': {}}
)

# Process the response
for event in response['Payload']:
    if 'Records' in event:
        try:
            print(event['Records']['Payload'].decode('utf-8'))
        except UnicodeDecodeError:
            print(event['Records']['Payload'].decode('utf-8', errors='replace'))
