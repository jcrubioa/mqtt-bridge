import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
import json
import time
import paho.mqtt.client as mqtt #import the client1

def on_message(client, userdata, message):
  payload = str(message.payload.decode("utf-8"))
  print("message received " , payload)
  print("message topic=", message.topic)
  print("message qos=", message.qos)
  print("message retain flag=", message.retain)
  print("Raw Payload: {}".format(payload))
  write_to_db(payload)


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

  #cursor.close()



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
