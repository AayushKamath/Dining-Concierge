import json
import boto3
import datetime
from time import localtime, strftime
import time
from zoneinfo import ZoneInfo
def form_response(slot_to_elicit, current_slots, accept):
    response = {
        "sessionState": {
            "dialogAction": {
                "type": "ElicitSlot",
                "slotToElicit": slot_to_elicit,
            },
            "intent": {
                "confirmationState": "Confirmed" if accept else "Denied" ,
                "name": "DiningSuggestionsIntent",
                "slots": current_slots,
            }
        }
    }
    return response

nones = ['None', None]
# client = boto3.client('lexv2-runtime')
def lambda_handler(event, context):
    print(event)
    slots = event["sessionState"]["intent"]["slots"]
    if slots["Location"] in nones:
        response = form_response("Location", slots, True)
    elif slots["Cuisine"] in nones:
        response = form_response("Cuisine", slots, True)
    elif slots["NumPeople"] in nones:
        if slots["Cuisine"]["value"]["resolvedValues"][0].lower() not in ['japanese', 'italian', 'chinese', 'mexican', 'indian', 'american']:
            response = form_response("Cuisine", slots, False)
        else:
            response = form_response("NumPeople", slots, True)
    elif slots["DiningDate"] in nones:
        # Do i need to conv to int?
        num_people = slots["NumPeople"]["value"]["resolvedValues"][0]
        if not num_people.isnumeric() or int(num_people) < 1:
            response = form_response("NumPeople", slots, False)
        else:
            response = form_response("DiningDate", slots, True)
    elif slots["DiningTime"] in nones:
        dining_date = slots["DiningDate"]["value"]["resolvedValues"][0]
        date = datetime.datetime.strptime(dining_date, "%Y-%m-%d").date()
        today_date = datetime.date.today()
        if date < today_date:
            response = form_response("DiningDate", slots, False)
        else:
            response = form_response("DiningTime", slots, True)
    elif slots["PhoneNumber"] in nones:
        dining_time = slots["DiningTime"]["value"]["resolvedValues"][0]
        dining_date = slots["DiningDate"]["value"]["resolvedValues"][0]
        dining_dt = dining_date + " " + dining_time
        dt = datetime.datetime.strptime(dining_dt, '%Y-%m-%d %H:%M')
        dining_datetime = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, 0, 0, ZoneInfo('America/New_York'))
        today_datetime = datetime.datetime.now(ZoneInfo('UTC'))
        print(dining_datetime)
        print(today_datetime)
        if dining_datetime < today_datetime:
            response = form_response("DiningTime", slots, False)
        else:
            response = form_response("PhoneNumber", slots, True)
    else:
        phone_number = slots["PhoneNumber"]["value"]["resolvedValues"][0]
        if len(phone_number) != 10:
            response = form_response("PhoneNumber", slots, False)
        else:
            response = {
        "sessionState": {
                "dialogAction": {
                    "type": "Close",
                },
                "intent": {
                    "confirmationState": "Confirmed",
                    "name": "DiningSuggestionsIntent",
                    "slots": slots,
                    "state": "Fulfilled"
                }
            }
        }
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName='q1')
        message = {
            "Location": slots["Location"]["value"]["resolvedValues"][0],
            "Cuisine": slots["Cuisine"]["value"]["resolvedValues"][0],
            "NumPeople": slots["NumPeople"]["value"]["resolvedValues"][0],
            "DiningDate": slots["DiningDate"]["value"]["resolvedValues"][0],
            "DiningTime": slots["DiningTime"]["value"]["resolvedValues"][0],
            "PhoneNumber": slots["PhoneNumber"]["value"]["resolvedValues"][0]
        }
        print(message)
        queue.send_message(MessageBody=json.dumps(message))
    return response

