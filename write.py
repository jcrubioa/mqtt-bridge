import time
import datetime
import json
import uuid
import random
import paho.mqtt.client as mqtt #import the client1

broker_address="localhost"

import random
import time

def strTimeProp(start, end, format, prop):
    """Get a time at a proportion of a range of two formatted times.

    start and end should be strings specifying times formated in the
    given format (strftime-style), giving an interval [start, end].
    prop specifies how a proportion of the interval to be taken after
    start.  The returned time will be in the specified format.
    """

    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(format, time.localtime(ptime))


def randomDate(start, end, prop):
    return strTimeProp(start, end, '%Y-%m-%d %H:%M', prop)

print("creating new instance")
client = mqtt.Client("P2") #create new instance
client.connect(broker_address)
ts = time.time()
points = [
	(5.217041, -73.542104, "Río Bogotá Nacedero", "1"),
	(5.150290, -73.690503, "Río Bogotá Chocontá", "2"),
	(4.750229, -74.127806, "Río Bogotá Siberia", "3"),
	(-1.418765, -70.587582, "Río Caquetá Zona Media", "4")
]

while True:
	print("Creating new data point...")
	#timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
	timestamp = randomDate('2019-06-01 00:00', '2019-06-17 12:00', random.random())
	event_uuid = str(uuid.uuid4())

	point = random.choice(points)

	payload = {
		'measure_id': event_uuid,
		'device_id': point[3],
		'description': point[2],
		'lat': point[0],
		'lng': point[1],
		'event_timestamp': timestamp,
		'metrics': [
			{
				'name': 'temperature',
				'value': random.uniform(2, 35)
			},
			{
				'name': 'turbidity',
				'value': random.uniform(0, 1)
			}
		]
	}

	print("Payload: {}".format(json.dumps(payload)))
	print("Publishing message to topic", "sensor_data")
	client.publish("sensor_data", json.dumps(payload))
