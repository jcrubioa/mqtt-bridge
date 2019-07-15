import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
import paho.mqtt.client as mqtt #import the client1
import json
import time

import sqlite3
import requests
from sqlite3 import Error


def on_message(client, userdata, message):
  payload = str(message.payload.decode("utf-8"))
  print("message received " , payload)
  print("message topic=", message.topic)
  print("message qos=", message.qos)
  print("message retain flag=", message.retain)
  print("Raw Payload: {}".format(payload))
  write_to_db(payload)
  check_rules(payload)

def send_mail(rule, send_to, point_name):
  url = "https://api.sendinblue.com/v3/smtp/email"
  to = []
  for receiver in send_to:
    to.append(
      {
        "email": receiver
      }
    )
  sensed_value = eval(rule['metric'])
  payload = {
    "sender":{
      "name":"no_reply_UNAL",
      "email":"jcrubioa@unal.edu.co"
    },
    "to": to,
    "textContent":"La variable \"{metric}\" ha salido del umbral establecido: \"{metric} {comparator} {value}\".\n Valor sensado: \"{sensed_value}\""\
      .format(metric=rule['metric'], comparator=rule['comparator'], value=rule['value'], sensed_value=round(sensed_value, 2)),
    "subject": "Alerta Preventiva: Valor sensado en el dispositivo \"{point_name}\" fuera de los umbrales establecidos".format(point_name=point_name)
  }
  #"replyTo":{"email":"jcrubioa2@gmail.com"}
  headers = {
      'accept': "application/json",
      'content-type': "application/json",
      'api-key': "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"
      }
  print(payload)
  response = requests.request("POST", url, data=json.dumps(payload), headers=headers)
  print(response.text)

def check_rules(payload):
  print('check_rules')
  data = json.loads(payload)
  for metric in data['metrics']:
    exec('{} = {}'.format(metric['name'], metric['value']), globals())
    print('{} = {}'.format(metric['name'], metric['value']))
  with open('/conf/rules.json') as json_file:
    rules_config = json.load(json_file)
    device_id = data['device_id']
    for rule in rules_config['rules'][device_id]['thresholds']:
      rule_expression = "{} {} {}".format(rule['metric'], rule['comparator'], rule['value'])
      triggered = eval(rule_expression)
      if triggered:
        send_to = rules_config['rules'][device_id]['emails']
        point_name = rules_config['rules'][device_id]['point_name']
        send_mail(rule, send_to, point_name)



def write_to_db(payload):
  data = json.loads(payload)
  print("Payload: {}".format(data))
  measure_query_schema = """ 
                        INSERT INTO `measure`
                        (measure_id, device_id, description, lat, lng, event_timestamp)
                        VALUES ('{}','{}', '{}', {}, {}, '{}')
                      """
  metric_query_schema = """ 
                        INSERT INTO `metric`
                        (measure_id, metric_name, metric_value)
                        VALUES ('{}','{}', {})
                      """
  try:
    measure_query = measure_query_schema.\
      format(data['measure_id'], data['device_id'], data['description'], data['lat'], data['lng'], data['event_timestamp'])
    measure_query_result  = cursor.execute(measure_query)
    
    for metric in data['metrics']:
      metric_query = metric_query_schema.\
        format(data['measure_id'], metric['name'], metric['value'])
      metric_query_result = cursor.execute(metric_query)
    
    print ("Measure inserted successfully")
    connection.commit()
  except mysql.connector.Error as error :
    connection.rollback() #rollback if any exception occured
    print("Failed inserting record into python_users table {}".format(error))



connection = mysql.connector.connect(host='35.196.177.135',
                               database='water_quality',
                               user='root',
                               password='tpi2019')
cursor = connection.cursor()

broker_address="broker"
#broker_address="iot.eclipse.org"
print("creating new instance")
client = mqtt.Client("P1") #create new instance
client.on_message=on_message #attach function to callback
print("connecting to broker")
client.connect(broker_address) #connect to broker
client.loop_start() #start the loop
print("Subscribing to topic", "sensor_data")
client.subscribe("sensor_data")
while True: time.sleep(0.1) # wait
#client.loop_stop() #stop the loop

connection.close()
print("MySQL connection is closed")
