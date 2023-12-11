from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key
import boto3
import json
import random

def lambda_handler(event, context):

    host = 'search-restaurants-7jhorj5amgppyfj7mnfcxciozy.us-east-1.es.amazonaws.com'
    region = 'us-east-1'

    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth('AKIA4KX33PBCXPFRUT47', 'I7yby6qviS7cb6aNOtFHs7DHrbtOEuocRJ3mCn6f', region, service)

    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/847710484549/q1'
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'Location', 'Cuisine', 'NumPeople', 'DiningDate', 'DiningTime', 'PhoneNumber'
        ],
        MaxNumberOfMessages=1,
    )
    if not 'Messages' in response:
        return
    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )

    message_body = json.loads(message['Body'])

    cuisine = (message_body['Cuisine']).lower()

    client = OpenSearch(
        hosts = [{'host': host, 'port': 443}],
        http_auth = awsauth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )

    query = {
        "query": {
            "match_phrase": {"cuisine": cuisine.capitalize()}
        }
    }

    target_ids = []
    resp = client.search(index="restaurants", size=1000, body=query)
    for hit in resp['hits']['hits']:
        target_ids.append(hit["_source"]['id'])

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')
    
    id = random.choice(target_ids)
    response = table.query(
        KeyConditionExpression=Key('id').eq(id)
    )
    name = response['Items'][0]['name']
    address = response['Items'][0]['address']['address1']
    city = response['Items'][0]['address']['city']
    state = response['Items'][0]['address']['state']
    zip = response['Items'][0]['address']['zip_code']
    rating = response['Items'][0]['rating']
    message = f'Based on your preference for {cuisine.capitalize()} cuisine, we recommend {name}, at {address}, {city}, {state} {zip}, with a rating of {rating}'


    client = boto3.client('sns')
    response = client.publish(
        TargetArn='arn:aws:sns:us-east-1:847710484549:restaurants',
        Message=json.dumps(message)
    )
