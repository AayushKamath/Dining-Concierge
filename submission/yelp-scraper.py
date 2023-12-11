import json
import boto3
from botocore.exceptions import ClientError
import requests
import time
from decimal import Decimal

def insert_data(data_list, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    # overwrite if the same index is provided
    for data in data_list:
        response = table.put_item(Item=data)
    print('@insert_data: response', response)
    return response
def lookup_data(key, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    try:
        response = table.get_item(Key=key)
    except ClientError as e:
        print('Error', e.response['Error']['Message'])
    else:
        print(response['Item'])
        return response['Item']
def update_item(key, feature, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    # change student location
    response = table.update_item(
        Key=key,
        UpdateExpression="set #feature=:f",
        ExpressionAttributeValues={
            ':f': feature
        },
        ExpressionAttributeNames={
            "#feature": "from"
        },
        ReturnValues="UPDATED_NEW"
    )
    print(response)
    return response
def delete_item(key, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    try:
        response = table.delete_item(Key=key)
    except ClientError as e:
        print('Error', e.response['Error']['Message'])
    else:
        print(response)
        return response

API_KEY='FiSUmwFNmsk7FxBqOJySPKwwmvgM5YQ8gaavOARTusM6twdoKTyH8keotmNLUZxQ4U6nAU7GClpNXwtdrOe5nIOnIaF3UIgNDfNpAF9nVNZViMWVwvFBCWkgILAUYnYx'
headers = {'Authorization': 'Bearer %s' % API_KEY}

url='https://api.yelp.com/v3/businesses/search'

# params = {'term':'seafood','location':'New York City'}
cuisines = ['Japanese', 'Italian', 'Chinese', 'Mexican', 'Indian', 'American']


seen_ids = []
table_data = []


for cuisine in cuisines:
    for offset in range(0, 20):
        params = {'term':f'{cuisine} food', 'location': 'New York', 'limit': 50, 'offset': offset*50}

        # Making a get request to the API
        req=requests.get(url, params=params, headers=headers)

        # proceed only if the status code is 200
        print('The status code is {}'.format(req.status_code))
        if req.status_code != 200:
            continue

        result = json.loads(req.text)
        businesses = result['businesses']
        print(len(businesses))
        for business in businesses:
            if business['id'] in seen_ids:
                continue
            data = {
                'id': business['id'],
                'name': business['name'],
                'address': business['location'],
                'coordinates': business['coordinates'],
                'review_count': business['review_count'],
                'rating': business['rating'],
                'zip_code': business['location']['zip_code'],
                'cuisine': cuisine,
                'insertedAtTimestamp': time.time()
            }
            table_data.append(data)
            seen_ids.append(business['id'])
table_data = json.loads(json.dumps(table_data), parse_float=Decimal)
print(len(table_data))
print("inserting")
insert_data(table_data)