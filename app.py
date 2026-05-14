import os
import time
import json
import signal
import socket
import pika
import logging
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join('logs', 'backend.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', 3306))
DB_USER = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_NAME = os.getenv('DB_NAME', 'jobs')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE', 'job_queue')
RUN_ONCE = os.getenv('RUN_ONCE', 'false').lower() == 'true'

try:
    HOST_ID = socket.gethostname() + '_' + socket.gethostbyname(socket.gethostname())
except socket.gaierror:
    HOST_ID = socket.gethostname() + '_unknown-ip'

shutdown_flag = False

def signal_handler(signum, frame):
    global shutdown_flag
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    shutdown_flag = True

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

def get_db_connection():
    conn = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    return conn

def update_job_status(job_id, status):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE jobs SET status = %s WHERE id = %s', (status, job_id))
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Job {job_id}: status updated to '{status}'")
    except Exception as e:
        logger.error(f"Job {job_id}: failed to update status to '{status}': {e}")

def process_job(ch, method, body):
    try:
        job_data = json.loads(body)
        job_id = job_data.get('id')
        creator = job_data.get('creator')
        process_time = job_data.get('process_time')

        if not job_id or not process_time:
            logger.error(f"Invalid job data: {job_data}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        logger.info(f"Processing job {job_id}: creator={creator}, process_time={process_time}s, worker={HOST_ID}")

        update_job_status(job_id, 'processing')

        logger.info(f"Job {job_id}: sleeping for {process_time} seconds...")
        for i in range(process_time):
            if shutdown_flag:
                logger.info(f"Job {job_id}: shutdown requested, stopping work")
                update_job_status(job_id, 'error')
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                return
            time.sleep(1)

        update_job_status(job_id, 'completed')
        logger.info(f"Job {job_id}: completed successfully")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        job_id = '(unknown)'
        try:
            job_data = json.loads(body)
            job_id = job_data.get('id', '(unknown)')
        except Exception:
            pass
        logger.error(f"Job {job_id}: error processing job: {e}")
        try:
            update_job_status(job_id, 'error')
        except Exception:
            pass
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main():
    logger.info(f"Starting worker on {HOST_ID}")
    logger.info(f"DB={DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}, RABBITMQ_HOST={RABBITMQ_HOST}, QUEUE={RABBITMQ_QUEUE}, RUN_ONCE={RUN_ONCE}")

    while not shutdown_flag:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, heartbeat=600)
            )
            channel = connection.channel()
            channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

            channel.basic_qos(prefetch_count=1)

            logger.info("Connected to RabbitMQ, waiting for jobs...")

            for method_frame, properties, body in channel.consume(queue=RABBITMQ_QUEUE, inactivity_timeout=1):
                if shutdown_flag:
                    break

                if method_frame is None:
                    if RUN_ONCE:
                        logger.info("RUN_ONCE mode: no messages available, exiting")
                        break
                    continue

                process_job(channel, method_frame, body)

                if RUN_ONCE:
                    logger.info("RUN_ONCE mode: job processed, exiting")
                    break

            channel.cancel()
            connection.close()

        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelWrongStateError, ConnectionError) as e:
            logger.error(f"RabbitMQ connection error: {e}")
            if RUN_ONCE:
                logger.error("RUN_ONCE mode: cannot connect to RabbitMQ, exiting")
                break
            logger.info("Retrying in 5 seconds...")
            for _ in range(5):
                if shutdown_flag:
                    break
                time.sleep(1)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            break

    logger.info("Worker shut down complete")

if __name__ == '__main__':
    main()
