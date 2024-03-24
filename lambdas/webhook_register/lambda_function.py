import json
import boto3

dynamodb = boto3.client('dynamodb')

def lambda_handler(event, context):
    try:
        payload = json.loads(event['body'])

        item = {
            'pk': {'S': payload['stream']},
            'sk': {'S': payload['id']},
            'url': {'S': payload['url']}
        }

        dynamodb.put_item(
            TableName='webhooks_ddb_table', # "from" env
            Item=item
        )

        return {
            'statusCode': 200,
            'body': json.dumps('Service registered successfully')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
