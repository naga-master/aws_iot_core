#!/usr/bin/python3

import sys
import signal
import time
import json
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

createMQTTClient = AWSIoTMQTTClient("awsgeek")
createMQTTClient.configureEndpoint('XXXXXX-ats.iot.us-east-2.amazonaws.com', 443)

# Check these certificate names
createMQTTClient.configureCredentials("certs/AmazonRootCA1.crt", "certs/XXXX-private.pem.key", "certs/XXXXX-certificate.pem.crt")

createMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
createMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
createMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
createMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec

connection=createMQTTClient.connect()
print("Connected {}".format(connection))
thing_name="Demo_Thing"

default_job_topic_prefix="$aws/things/"+thing_name+"/jobs/"
status_topic="$aws/things/"+thing_name+"/status"
notify_topic=default_job_topic_prefix+"notify-next"

subscribe_topic=['awsgeek_topic',notify_topic] #topics to subscribe
publish_topic='awsgeek_publish'  #topic for publish

def update_status(job_id,status):
   update_topic =default_job_topic_prefix+job_id+"/update"
   print(update_topic)
   publish_message(update_topic,status) #call publish function

        

def unsubscribe_topics():
    """Unbsubscribes from AWS IoT topics before exiting
    """
    print("Unsubscribing")
    topics = subscribe_topics

    for topic in topics:
        createMQTTClient.unsubscribe(topic) #unsubscribe topic 

def subscribe_topics(sub_topics):
    """subscribes from AWS IoT topics
    """
    print("subscribing")

    topics = sub_topics

    for topic in topics:
        createMQTTClient.subscribe(topic,1,driveCallback) #subscribe topic
        print("Listening on {}".format(topic))


# Interrupt Handler useful to break out of the script
def interrupt_handler(signum, frame):
    unsubscribe_topics()
    sys.exit("Exited and unsubscribed")  #stop the python code

# Custom MQTT message callbacks
def driveCallback(client, userdata, message):
    '''
    receive the messages from subscribed topics
    '''
    print("Received {} from {}".format(message.payload,message.topic)) #print received message
    try:
        payload = json.loads(message.payload.decode('utf-8'))
        #print(payload)
        if 'execution' in payload:
            operationType = payload['execution']['jobDocument']['operation']
            job_id = payload['execution']['jobId']
            update_status(job_id,"IN_PROGRESS")
            if operationType == "heartbeat":
                print ("job started")
                publish_message(publish_topic,'I am alive at : {}'.format(round(time.time())))

                update_status(job_id,"SUCCEEDED")  #update the IoT Job to successfully finished job

    except Exception as msg:
        print("error in callback {}".format(msg))
        update_status(job_id,"FAILED")   #update the IoT Job to job execution failed 
    


# Subscribe to topics
subscribe_topics(subscribe_topic)

def publish_message(topic,data):
    '''
    publish message to the topic
    '''
    json_data={'status':data}
    payload=json.dumps(json_data)#pack the json data to publish
  
    resp=createMQTTClient.publish(topic,payload,0)
    print(resp)



while True:
    signal.signal(signal.SIGINT, interrupt_handler)
    time.sleep(1)

#unsubscribe_topics()
