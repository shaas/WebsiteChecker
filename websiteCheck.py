from dotenv import load_dotenv
from WebsiteChecker import website
from WCKafka import KafkaProducer

import os
import signal
import sys
import logging
import time
import threading

logger = logging.getLogger("WebsiteCheck")
num_threads = 0
thread_running = False
# Lock for the Website-List
lock_ws = threading.Lock()
# Lock for thread-handling
lock_threads = threading.Lock()
websites = []


# Signal-handler for SIGINT and SIGTERM
def handler_int(signum, frame):
    logger.info("WebsiteCheck got interupted")
    # simply exit
    sys.exit(0)


# Measure the websites and publish the results to Kafka
def check_websites():
    global lock_ws, lock_threads, thread_running, num_threads, websites
    lock_threads.acquire()
    num_threads += 1
    thread_running = True
    lock_threads.release()

    # get all necessary config-values from the environemt
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVER")
    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL")
    ssl_cafile = os.getenv("KAFKA_SSL_CAFILE")
    ssl_certfile = os.getenv("KAFKA_SSL_CERTFILE")
    ssl_keyfile = os.getenv("KAFKA_SSL_KEYFILE")

    topic = os.getenv("WC_TOPIC")

    # start the KafkaProducer
    producer = KafkaProducer.WcKafkaProducer(bootstrap_servers,
                                             security_protocol, ssl_cafile,
                                             ssl_certfile, ssl_keyfile)

    # create a new topic
    producer.create_topic(topic)
    # get measurements of the configured websites
    while True:
        # website-list is safed with a lock to prevent simultaneous access
        lock_ws.acquire()
        for ws in websites:
            ws.get_response()
            # publish results
            producer.send(topic, ws.as_json().encode("utf-8"))
        lock_ws.release()
        producer.flush()
        # measure every 10 seconds
        time.sleep(int(os.getenv("POLL_INTERVALL")))

    lock_threads.acquire()
    num_threads -= 1
    lock_threads.release()


# get all websites which should get checked
# for simplicity this is hardcoded in this demo.
def read_websites():
    global lock_ws, lock_threads, thread_running, num_threads, websites
    lock_threads.acquire()
    num_threads += 1
    thread_running = True
    lock_threads.release()

    ws1 = website.Website("https://vpn.haas.works", regex="Werben")
    ws2 = website.Website("http://www.google.de", regex="Werben")
    ws3 = website.Website("http://www.haribo.de")

    # website-list is safed with a lock to prevent simultaneous access
    lock_ws.acquire()
    websites.append(ws1)
    websites.append(ws2)
    websites.append(ws3)
    lock_ws.release()

    lock_threads.acquire()
    num_threads -= 1
    lock_threads.release()


def main():
    # load environment configuration from .env file
    load_dotenv(verbose=True)

    # set the loglevel
    loglevel = getattr(logging, os.getenv("LOGLEVEL").upper())
    if not isinstance(loglevel, int):
        print(f"{os.getenv('LOGLEVEL')} as LOGLEVEL is not valid. "
              "Using WARNING instead")
        loglevel = getattr(logging, "WARNING")
    logging.basicConfig(level=loglevel)

    # set signal handlers
    signal.signal(signal.SIGINT, handler_int)
    signal.signal(signal.SIGTERM, handler_int)

    # create and start website-reader thread
    reader_thread = threading.Thread(target=read_websites)
    reader_thread.start()
    # create and start website-checker thread
    checker_thread = threading.Thread(target=check_websites)
    checker_thread.start()


main()
