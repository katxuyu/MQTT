import paho.mqtt.client as mqtt
import logging,time

import threading, json
import requests as req

import config as secret
from MQTTClient import MQTTClient
from datetime import datetime
import sys

clients=[]
threads=[]

logging.basicConfig(\
   filename='mqtt-bridge.log',level=logging.INFO)

### CALLBACK WHEN CONNECTED SUCCESFULLY TO THE CLIENT ###
## When Connected to TTI, subcribe to the topic "#" ##
def on_connect(client, userdata, flags, rc):
   
   
   if rc==0:
      
      client.connected_flag=True 
      client.bad_connection_flag=False
      if client.sub_topic!="": 
         f = open(f"mqtt-bridge.log", "a")
         f.write(f"\n{datetime.now()} - Subscribing to {client.sub_topic} - BROKER: {client.broker}")
         f.close()
         print("subscribing to ",\
               client.sub_topic, "broker ",client.broker)
         topic=client.sub_topic
         client.subscribe(topic,client.sub_qos)
      elif client.sub_topics!="":
         f = open(f"mqtt-bridge.log", "a")
         f.write(f"\n{datetime.now()} - Subscribing to {client.sub_topics} - BROKER: {client.broker}")
         f.close()
         client.subscribe(client.sub_topics)

   else:
     f = open(f"error.log", "a")
     f.write(f"\n{datetime.now()} - Set bad connection flag {rc}")
     f.close()
     client.bad_connection_flag=True
     client.bad_count +=1
     client.connected_flag=False

### CALLBACK UPON TOPIC SUBSCRIPTION ###
def on_subscribe(client,userdata,mid,granted_qos):
   """removes mid values from subscribe list"""
   f = open(f"mqtt-bridge.log", "a")
   f.write(f"\n{datetime.now()} - In on subscribe callback result {mid}")
   f.close()
   client.subscribe_flag=True

### CALLBACK WHEN RECEIVING MESSAGES ###
## Emmediately route messages to the target BROKER ##
def on_message(client, userdata, msg):
      topic=secret.PUB_TOPIC
      m_decode=str(msg.payload.decode("utf-8","ignore"))
      f = open(f"mqtt-bridge.log", "a")
      f.write(f"\n{datetime.now()} - Message received from  {client.broker}")
      f.close()
      message_routing(client,topic,m_decode)



### INITIALIZE EACH CLIENT ###
def Initialise_clients(cname,mqttclient_log=False,cleansession=True,flags=""):
   f = open(f"mqtt-bridge.log", "a")
   f.write(f"\n{datetime.now()} - Initializing clients {cname}")
   f.close()
   client= MQTTClient(cname,clean_session=cleansession)
   client.cname=cname
   client.on_connect= on_connect  
   client.on_message=on_message 
   client.on_subscribe=on_subscribe
   if mqttclient_log:
      client.on_log=logging.INFO
   return client

# ### PUBLISH MESSAGE TO THE TARGET BROKER ###
# def message_routing(client,topic,msg):
#    clientname=client.cname
#    print("in filter ",clientname)
#    if client.connector=="c1":
#       client_c2.publish(topic,msg)
         
#    if client.connector=="c2":
#       client_c1.publish(topic,msg)
TO_CTR = 0
def message_routing(client,topic,msg):
   global TO_CTR
   try:
      clientname=client.cname
      f = open(f"mqtt-bridge.log", "a")
      f.write(f"\n{datetime.now()} - Sending payload to {client.broker} {TO_CTR}")
      f.close()
      header  = {'Authorization': secret.AUTH}
      res = req.post(url, json=json.loads(msg), headers=header, timeout=30)
      TO_CTR = 0
   except Exception as e:
      if TO_CTR != 5:
         TO_CTR += 1
         f = open(f"error.log", "a")
         f.write(f"\n{datetime.now()} - TIMEOUT RETRY {TO_CTR}")
         f.close()
         message_routing(client,topic,msg)
      else:
         f = open(f"error.log", "a")
         f.write(f"\n{datetime.now()} - {e}")
         f.close()
         client_c2.loop_stop()
         client_c1.loop_stop()
         MQTTClient.run_flag=False
         time.sleep(5)
         for client in clients:
            client.disconnect()
         exit()
         
         

   

   

### MAIN CODE ###
MQTTClient.run_flag=True
now=time.time()
count=0

### Broker 1 and Broker 2 setup ###
client_c1 =Initialise_clients(secret.CNAME1)
client_c1.sub_topic=secret.SUB_TOPIC
client_c1.broker=secret.BROKER1
client_c1.tls_set(tls_version=mqtt.ssl.PROTOCOL_TLS)
client_c1.username_pw_set(secret.BKR1_URN, secret.BKR1_PWD)
client_c1.port = secret.PORT1
clients.append(client_c1)

client_c2 =Initialise_clients(secret.CNAME2)
client_c2.broker=secret.BROKER2
client_c2.port = secret.PORT2
client_c2.tls_set(tls_version=mqtt.ssl.PROTOCOL_TLS)
client_c2.username_pw_set(secret.BKR2_URN, secret.BKR2_PWD)
client_c2.sub_topic=""
clients.append(client_c2)

client_c1.connector="c1" 
client_c2.connector="c2"

url = secret.HTTP_URL

for client in clients:
   f = open(f"mqtt-bridge.log", "a")
   f.write(f"\n{datetime.now()} - Connecting to broker {str(client.broker)}")
   f.close()
   try:
        res=client.connect(client.broker,client.port,client.keepalive)
        client.loop_start()

   except Exception as e:
        f = open(f"error.log", "a")
        f.write(f"\n{datetime.now()} - {e}")
        f.close()
      #   logging.debug("connection failed")
      #   print("connection failed", client.broker)
        client.bad_count +=1
        client.bad_connection_flag=True
   
f = open(f"mqtt-bridge.log", "a")
f.write(f"\n\n{datetime.now()} - START: Initializing")
f.close()
try:
   while MQTTClient.run_flag:
      time.sleep(1)
except:
   pass
client_c2.loop_stop()
client_c1.loop_stop()
MQTTClient.run_flag=False
time.sleep(5)
for client in clients:
   client.disconnect()



