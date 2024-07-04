# AWS CLI KINESIS COMMANDS


**aws kinesis list-streams --region us-west-2**
{
    "StreamNames": [
        "ExampleInputStream-RCF",
        "ExampleOutputStream-RCF"
    ],
    "StreamSummaries": [
        {
            "StreamName": "ExampleInputStream-RCF",
            "StreamARN": "arn:aws:kinesis:us-west-2:635470522716:stream/ExampleInputStream-RCF",
            "StreamStatus": "ACTIVE",
            "StreamModeDetails": {
                "StreamMode": "PROVISIONED"
            },
            "StreamCreationTimestamp": "2024-06-08T07:45:15+00:00"
        },
        {
            "StreamName": "ExampleOutputStream-RCF",
            "StreamARN": "arn:aws:kinesis:us-west-2:635470522716:stream/ExampleOutputStream-RCF",
            "StreamStatus": "ACTIVE",
            "StreamModeDetails": {
                "StreamMode": "PROVISIONED"
            },
            "StreamCreationTimestamp": "2024-06-08T07:45:14+00:00"
        }
    ]
}

**aws kinesis list-shards --stream-name ExampleInputStream-RCF --region us-west-2**
{
    "Shards": [
        {
            "ShardId": "shardId-000000000000",
            "HashKeyRange": {
                "StartingHashKey": "0",
                "EndingHashKey": "340282366920938463463374607431768211455"
            },
            "SequenceNumberRange": {
                "StartingSequenceNumber": "49652800501372648887727493073052637838139012874277748738"
            }
        }
    ]
}


**aws kinesis get-shard-iterator --stream-name ExampleInputStream-RCF --shard-id shardId-000000000000 --shard-iterator-type TRIM_HORIZON --region us-west-2**
{
    "ShardIterator": "AAAAAAAAAAEbhTXCK/p6X50itbYg+kPH4900dPDmzk6zJ6NAbKm/Ob/cy/SpTkw8ZXGbw7yLBTIAPf9/r7kULkiY5oiii0xwRAI+3FMB9kFaN+dx9dnAXi/Szu+iBw+GaBXfA3GApq8l01x7aKJhUdfd1m7LIzsnnFK2GboCAEDujcTXj5rXaXV3JrZsgDTN4loo7NtQw57DlhUWWwylWMPIb/ppgcsnVHfkFBb0xav5gJ6GLnzbYvGwUFut+Rl/300265Xnr+s="
}

**aws kinesis get-records --shard-iterator "AAAAAAAAAAEbhTXCK/p6X50itbYg+kPH4900dPDmzk6zJ6NAbKm/Ob/cy/SpTkw8ZXGbw7yLBTIAPf9/r7kULkiY5oiii0xwRAI+3FMB9kFaN+dx9dnAXi/Szu+iBw+GaBXfA3GApq8l01x7aKJhUdfd1m7LIzsnnFK2GboCAEDujcTXj5rXaXV3JrZsgDTN4loo7NtQw57DlhUWWwylWMPIb/ppgcsnVHfkFBb0xav5gJ6GLnzbYvGwUFut+Rl/300265Xnr+s=" --region us-west-2**
{
    "Records": [],
    "NextShardIterator": "AAAAAAAAAAHVEgXTnLfg+e/MEPY0VJtYh5iFpsmP+bwzVEjAh5nAaz8+gf7ZXPUBsD2ZMEFbIg3NG3g/skgCH7Xok8SpKxbb6mAwySOF4N36Ljd2rkieAeBhQhdde+4bLL88IL8CqSdx9kRF+1thPN32HOfKTtns4nx89vjZ2Y8QLwu71qJj4T8KizyIlP6pzwd7wdZrNQebNb/bIFPYZzKtIKOis8pAbKxho7u+djf8SheiML0J+kXZbLaBYvneHqpSZA39pbM=",
    "MillisBehindLatest": 67998000
}