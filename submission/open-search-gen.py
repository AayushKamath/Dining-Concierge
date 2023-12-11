import json
import boto3
from botocore.exceptions import ClientError
import requests

API_KEY='FiSUmwFNmsk7FxBqOJySPKwwmvgM5YQ8gaavOARTusM6twdoKTyH8keotmNLUZxQ4U6nAU7GClpNXwtdrOe5nIOnIaF3UIgNDfNpAF9nVNZViMWVwvFBCWkgILAUYnYx'
headers = {'Authorization': 'Bearer %s' % API_KEY}

url='https://api.yelp.com/v3/businesses/search'

# params = {'term':'seafood','location':'New York City'}
cuisines = ['Japanese', 'Italian', 'Chinese', 'Mexican', 'Indian', 'American']

seen_ids = []
with open("opensearch.json", "w") as f:
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
                next_id = len(seen_ids)
                
                line1 = {
                    "index": {
                        "_index": "restaurants", 
                        "_id": "{}".format(next_id)
                    }
                }
                line2 = {
                    "id": business['id'],
                    "cuisine": cuisine
                }
                f.write(json.dumps(line1) + "\n")
                f.write(json.dumps(line2) + "\n")
                seen_ids.append(business['id'])