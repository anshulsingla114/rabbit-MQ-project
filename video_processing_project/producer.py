import pika
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

def send_video_task(video_path):
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue='video_tasks')

    channel.basic_publish(exchange='',
                          routing_key='video_tasks',
                          body=video_path)
    logging.info(f" [x] Sent '{video_path}'")
    connection.close()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python producer.py <path_to_video>")
        sys.exit(1)
    
    video_path = sys.argv[1]
    send_video_task(video_path)
