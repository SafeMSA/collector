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
tot_tags = 1
tag_dic = {}
# Callback to handle incoming messages
def tag_callback(ch, method, properties, body):
    global tag_dic, tot_tags
    message = json.loads(body.decode())  # Parse incoming JSON message
    print(f"Received: {message}")

    if message.get("tag") not in tag_dic:
        tag_dic[message.get("tag")] = f"tag{tot_tags}"
        tot_tags += 1

    with open(f'{tag_dic[message.get("tag")]}.txt', 'a') as file:
        file.seek(0, 2)
        if (file.tell() == 0):
            file.write("Time, Time to arrive, ID\n")
        file.write(f'{datetime.fromisoformat(message.get("time_sent")).strftime("%H:%M:%S")}, {2 + float(message.get("time_diff"))}, {message.get("id")}\n')
    
    # Acknowledge the received message
    ch.basic_ack(delivery_tag=method.delivery_tag)

def evac_callback(ch, method, properties, body):
    message = json.loads(body.decode())  # Parse incoming JSON message
    print(f"Received: {message}")

    if message.get("state") == "Down" or message.get("state") == "Up":
        with open(f'{message.get("name")}.txt', 'a') as file:
            file.seek(0, 2)
            if (file.tell() == 0):
                file.write("Time, State\n")
            file.write(f'{datetime.fromisoformat(message.get("time_sent")).strftime("%H:%M:%S")}, {message.get("state")}\n')

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