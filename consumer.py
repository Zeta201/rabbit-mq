import pika
import os
import json
import smtplib
from dotenv import load_dotenv
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email(to_email, subject, body):
    smtp_host = os.getenv("EMAIL_HOST")
    smtp_port = int(os.getenv("EMAIL_PORT", "587"))
    from_email = os.getenv("EMAIL_HOST_USER")
    password = os.getenv("EMAIL_HOST_PASSWORD")

    if not all([smtp_host, smtp_port, from_email, password]):
        print("Email environment variables are not fully set.")
        return

    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        server.login(from_email, password)
        server.send_message(msg)
        server.quit()
        print(f"Email sent to {to_email}")
    except Exception as e:
        print(f"Failed to send email: {e}")

def main():
    load_dotenv()

    username = os.getenv("USERNAME")
    vhost = os.getenv("VHOST")
    host = os.getenv("HOST")
    password = os.getenv("PASSWORD")
    queue_name = "TestQueue"
    exchange = "choreo"

    if username and vhost and host and password:
        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=host, port=5672, virtual_host=vhost, credentials=credentials, socket_timeout=5))
        channel = connection.channel()

        channel.exchange_declare(exchange=exchange, durable=True, exchange_type='topic')
        channel.queue_declare(queue=queue_name, durable=True)
        channel.queue_bind(exchange=exchange, queue=queue_name, routing_key="#")

        def callback(ch, method, properties, body):
            print(f"Message received: {body.decode()}")
            try:
                data = json.loads(body.decode())

                to_email = data.get("to_email")
                txn_type = data.get("type")
                amount = data.get("amount")
                status = data.get("status")
                account = data.get("accountNo")
                timestamp = data.get("timestamp")

                if to_email:
                    subject = f"Bank Transaction Alert: {txn_type.capitalize()}"
                    message = f"Your {txn_type} of ${amount} on account {account} was {status} at {timestamp}."
                    send_email(to_email, subject, message)
                else:
                    print("No email address in message.")

            except Exception as e:
                print(f"Error processing message: {e}")

        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
        print("Email consumer started. Waiting for messages...")
        channel.start_consuming()
    else:
        print("Missing RabbitMQ environment variables.")

if __name__ == '__main__':
    main()
