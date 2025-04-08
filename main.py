import pika
import socket
import time
import json
import os
from datetime import datetime
from collections import defaultdict

# Constants
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 9093
SUBSCRIBER_QUEUE = 'response_queue'  # Name of the queue to send the response to
EVACUATION_QUEUE = 'evac_info_queue'
USER = 'myuser'
PASSWORD = 'mypassword'

# Connect to RabbitMQ and declare the necessary queues
def connect_to_rabbitmq():
    credentials = pika.PlainCredentials(USER, PASSWORD)
    
    while True:
        try:
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST, 
                port=RABBITMQ_PORT,
                credentials=credentials, 
                blocked_connection_timeout=1
            )
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            # Declare the subscriber queue
            channel.queue_declare(queue=SUBSCRIBER_QUEUE, durable=True)
            channel.queue_declare(queue=EVACUATION_QUEUE, durable=True)

            return connection, channel
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelClosedByBroker) as error:
            print(f"Error connecting to RabbitMQ: {error}. Retrying in 5 seconds...")
            time.sleep(5)

# Callback to handle incoming messages
def tag_callback(ch, method, properties, body):
    message = json.loads(body.decode())  # Parse incoming JSON message
    print(f"Received: {message}")

    with open(f'{message.get("tag")}.txt', 'a') as file:
        file.seek(0, 2)
        if (file.tell() == 0):
            file.write("Time stamp,Time to arrive\n")
        file.write(f'{message.get("time_sent")}, {message.get("time_diff")}')
    
    # Acknowledge the received message
    ch.basic_ack(delivery_tag=method.delivery_tag)

def evac_callback(ch, method, properties, body):
    message = json.loads(body.decode())  # Parse incoming JSON message
    print(f"Received: {message}")

    with open(f'{message.get("name")}.txt', 'a') as file:
        file.seek(0, 2)
        if (file.tell() == 0):
            file.write("Time stamp, Status")
        file.write(f'{message.get("time_sent")}, {message.get("status")}')

def main():
    while True:
        try:
            # Connect to RabbitMQ
            connection, channel = connect_to_rabbitmq()

            # Start consuming messages from the subscriber queue
            channel.basic_consume(queue=SUBSCRIBER_QUEUE, on_message_callback=tag_callback)
            channel.basic_consume(queue=EVACUATION_QUEUE, on_message_callback=evac_callback)

            print("Waiting for messages. To exit press CTRL+C.")
            channel.start_consuming()

        except Exception as e:
            print(f"Error: {e}. Reconnecting...")
            time.sleep(5)

if __name__ == '__main__':
    main()