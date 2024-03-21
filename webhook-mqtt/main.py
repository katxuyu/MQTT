from flask import Flask, jsonify, request, Response
from datetime import datetime
import logging
import configparser
import time
import socket
import paho.mqtt.client as mqttClient

app = Flask(__name__)

config = configparser.ConfigParser()
config.read('config.ini')
	
Connected = False #global variable for the state of the connection

log_path = config.get('logs', 'path')
https_auth = config.get('this', 'auth')
broker_address= config.get('mqtt', 'broker_address')
port = config.get('mqtt', 'port')
user = config.get('mqtt', 'user')
password = config.get('mqtt', 'password')
topic = config.get('mqtt', 'topic')

logging.basicConfig(filename=log_path,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)



logger = logging.getLogger()
logger.setLevel(logging.INFO)

def on_connect(client, userdata, flags, rc):
    
    global Connected
    if rc == 0:
        #logging.info("Connected to broker")
        global Connected                #Use global variable
        Connected = True                #Signal connection 
 
    else:
        logging.error("Connection failed")

def send_payload(data):

    client = mqttClient.Client("Python")               #create new instance
    client.username_pw_set(username=user, password=password)    #set username and password
    client.on_connect= on_connect                      #attach function to callback
    client.connect(broker_address, port=int(port))          #connect to broker

    client.loop_start()        #start the loop
 
    while Connected != True:    #Wait for connection
        time.sleep(0.1)
    
    try:
        client.publish(topic,data)
    except KeyboardInterrupt:
        client.disconnect()
        client.loop_stop()
    else:
        client.disconnect()
        client.loop_stop()
        logging.info("Success")

app.run(host='20.24.19.71', port=5000, debug=True)

@app.route("/payloads", methods=['POST'])
def receive_payloads():
    
    auth = request.headers.get('Authorization')
    if not auth:
        return Response(
            "Authentication error: Authorization header is missing",
            status=401
        )
    parts = auth.split()

    if parts[0].lower() != "bearer":
        return Response("Authentication error: Authorization header must start with ' Bearer'", status=401)
    elif len(parts) == 1:
        return Response("Authentication error: Token not found", status=401)
    elif len(parts) > 2:
        return Response("Authentication error: Authorization header must be 'Bearer <token>'", status=401)
    
    elif auth != https_auth:
        return Response("Authentication error: Wrong Authorization", status=401)
    # payload = request.get_data().decode()
    # send_payload(payload)   
    
    try:
        payload = request.get_data().decode()
        logging.info(payload)
        send_payload(payload)
    except Exception as e:
        logger.error(f"ERROR: {e}")
        return Response(f"ERROR: {e}", status=501)
    
    return Response("Payload processed successfully.", status=200)


