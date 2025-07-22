import pika
import os

def main():
    username = os.getenv("USERNAME")
    vhost = os.getenv("VHOST")
    host = os.getenv("HOST")
    password = os.getenv("PASSWORD")
    queueName = "TestQueue"
    exchange = 'choreo'

    if username and vhost and host and password:
        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=host, port=5672, virtual_host=vhost, credentials=credentials, socket_timeout=5))

        channel = connection.channel()

        # Declare exchange and queue
        channel.exchange_declare(exchange=exchange, durable=True, exchange_type='topic')
        channel.queue_declare(queue=queueName, durable=True)

        # Bind queue to exchange with wildcard to receive all messages
        channel.queue_bind(exchange=exchange, queue=queueName, routing_key="#")

        # Callback for incoming messages
        def callbackFunctionForQueueA(ch, method, properties, body):
            print(f'Message received from Queue "{queueName}": {body.decode()}')

        # Start consuming
        channel.basic_consume(queue=queueName, on_message_callback=callbackFunctionForQueueA, auto_ack=True)

        print("Starting consumer session...")
        channel.start_consuming()
    else:
        print("One or more environment variables are not set.")

if __name__ == '__main__':
    main()
