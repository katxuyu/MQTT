import paho.mqtt.client as mqtt
import logging,time

### EXTEND PAHO CLIENT ###
class MQTTClient(mqtt.Client):
   run_flag=False 
   def __init__(self,cname,**kwargs):
      super(MQTTClient, self).__init__(cname,**kwargs)

      self.last_pub_time=time.time()
      self.topic_ack=[] 
      self.run_flag=True
      self.submitted_flag=False
      self.subscribe_flag=False
      self.bad_connection_flag=False
      self.bad_count=0
      self.connected_flag=False
      self.connect_flag=False
      self.disconnect_flag=False
      self.disconnect_time=0.0
      self.pub_msg_count=0
      self.pub_flag=False
      self.sub_topic=""
      self.sub_topics=""
      self.sub_qos=0
      self.devices=[]
      self.broker=""
      self.port=1883
      self.keepalive=60
      self.run_forever=False
      self.cname=""
      self.delay=10 #retry interval
      self.retry_time=time.time()