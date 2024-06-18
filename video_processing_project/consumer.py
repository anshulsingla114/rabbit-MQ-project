import pika
import cv2
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Define the base directory where the frames will be saved
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def process_video(ch, method, properties, body):
    video_path = body.decode()
    logging.info(f" [x] Received '{video_path}'")

    # Get the video's filename without extension and use it to create a unique directory for frames
    video_name = os.path.splitext(os.path.basename(video_path))[0]
    frames_dir = os.path.join(BASE_DIR, f'frames_{video_name}')
    os.makedirs(frames_dir, exist_ok=True)

    # Simple video processing example: extract frames
    cap = cv2.VideoCapture(video_path)
    count = 0
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break
        frame_filename = os.path.join(frames_dir, f"frame{count}.jpg")
        cv2.imwrite(frame_filename, frame)
        logging.info(f" [x] Saved frame {count} as '{frame_filename}'")
        count += 1

    cap.release()
    logging.info(f" [x] Done processing '{video_path}'")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def consume_video_tasks():
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue='video_tasks')

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='video_tasks', on_message_callback=process_video)

    logging.info(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    consume_video_tasks()
