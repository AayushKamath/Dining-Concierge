import json
import boto3

client = boto3.client('lexv2-runtime')
def lambda_handler(event, context):
    msg_from_user = event['messages'][0]['unstructured']['text']
    response = client.recognize_text(botId='4VMBMLK3V2',
                                botAliasId='KLSUU6BRJX',
                                localeId='en_US',
                                sessionId='testuser',
                                text=msg_from_user)

    msg_from_lex = response['messages'][0]['content']
    if msg_from_lex:
        print(f"Message from Chatbot: {msg_from_lex}")
        print(response)
        
        resp = {
            'statusCode': 200,
            'body': json.dumps('Test!'),
            'messages': [{
                'type': 'unstructured',
                'unstructured': {
                    'text': msg_from_lex
                }
            }]
        }
        return resp

