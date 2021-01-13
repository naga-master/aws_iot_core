#!/usr/bin/python3

import sys
import signal
import time
import json
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

createMQTTClient = AWSIoTMQTTClient("awsgeek")
createMQTTClient.configureEndpoint('XXXXX-ats.iot.us-east-2.amazonaws.com', 443)

# Check these certificate names
createMQTTClient.configureCredentials("certs/AmazonRootCA1.crt", "certs/XXXX-private.pem.key", "certs/XXXXX-certificate.pem.crt")

createMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
createMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
createMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
createMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec

connection=createMQTTClient.connect()
print("Connected {}".format(connection))

subscribe_topic=['awsgeek_topic','Testing'] #topics to subscribe
publish_topic='awsgeek_publish'  #topic for publish

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
    


# Subscribe to topics
subscribe_topics(subscribe_topic)

def publish_message(topic,data):
    '''
    publish message to the topic
    '''
    json_data={'message':data}
    payload=json.dumps(json_data)#pack the json data to publish
  
    resp=createMQTTClient.publish(topic,payload,0)
    print(resp)

publish_message(publish_topic,'Hello From AWS Geek')

while True:
    signal.signal(signal.SIGINT, interrupt_handler)
    time.sleep(1)

#unsubscribe_topics()
