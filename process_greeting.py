## Done as per Class's lab tutorial

import json
import boto3
import datetime

def lambda_handler(message, context):
    # 1. Log input message
    print("Received message from Step Functions:")
    print(message)
    
    # 2. Publish to SNS Topic
    sns_client = boto3.client('sns')

    ##########################
    # UPDATE THE BELOW LINE
    ##########################
    sns_topic_arn = 'arn:aws:sns:us-east-1:797919295461:TeleBotNotifier'

    response = sns_client.publish(
        TargetArn = sns_topic_arn,
        Message = json.dumps(
            {
                'default': json.dumps(message)
            }
        ),
        Subject = 'Tele ',
        MessageStructure = 'json'
    )
