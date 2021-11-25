import pika
import json
import os
import sys
import requests
from dotenv import load_dotenv
import time
import base64


load_dotenv()

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
DETECTOR_SERVICE_URL = os.getenv('DETECTOR_SERVICE_URL')
ISSUE_SERVICE_URL = os.getenv('ISSUE_SERVICE_URL')
INVENTORY_SERVICE_URL = os.getenv('INVENTORY_SERVICE_URL')
ISSUE_SERVICE_CLASSES = json.loads(os.getenv('ISSUE_SERVICE_CLASSES'))
INVENTORY_SERVICE_CLASSES = json.loads(os.getenv('INVENTORY_SERVICE_CLASSES'))

def main(ch, method, properties, body):
    details = json.loads(body)
    try:
        inventory_item = INVENTORY_SERVICE_CLASSES.__contains__(details['class'])
        issue_item = ISSUE_SERVICE_CLASSES.__contains__(details['class'])

        if(inventory_item):            
            inventory_details = {
                "name": details['title'],
                "code": "string", #oval id
                "vendor": "",
                "contact": "",
                "description": details['description']
            }
            # print(str(issue_details), file=sys.stderr)
            response = requests.post(INVENTORY_SERVICE_URL+'/v1/inventory', json=inventory_details)
            
        if(issue_item):
            issue_details = {
                "resource": details['reference'],
                "title": details['title'],
                "description": details['description'],
                "score": details['severity'],
                "issue_id": base64.b64encode(json.dumps(details['references']).encode()).decode(),
                "remediation_script": base64.b64encode(json.dumps(details['fixes']).encode()).decode(),
                "issue_date":details['issued_date'],
                "reference":details['scan_id']
            }
            # print(str(issue_details), file=sys.stderr)
            response = requests.post(ISSUE_SERVICE_URL+'/v1/issues', json=issue_details)

        
    except Exception as e:
        print('EXCEPTION'+str(sys.exc_info()), file=sys.stderr)
    # ch.basic_ack(delivery_tag = method.delivery_tag)

    
def subscribe():
    detection_classes_response = requests.get(DETECTOR_SERVICE_URL+'/v1/scans/classes')
    detection_classes = json.loads(detection_classes_response.text)    
   
    while(True):
        time.sleep(2.4)
        print('Consumer Starting...')
        connection = pika.BlockingConnection(pika.ConnectionParameters(os.environ['RABBITMQ_HOST']))
        channel = connection.channel()
        for classname in detection_classes['data']:
            channel.queue_declare(queue=classname['code'], durable=True)
            channel.basic_qos(prefetch_count=5)
            channel.basic_consume(queue=classname['code'], auto_ack=False, on_message_callback=main)
        
        try:
            channel.start_consuming()    
        except pika.exceptions.ConnectionClosedByBroker as e:
            print(e)
            continue
        except pika.exceptions.AMQPChannelError as err:
            print(err)
            break
        except pika.exceptions.AMQPConnectionError as e2:
            print(e2)
            continue
        except Exception as ex:
            break

print('Data Collection v1.0')
subscribe()
