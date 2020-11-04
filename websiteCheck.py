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
lock_ws = threading.Lock()
lock_threads = threading.Lock()
websites = []

def handler_int(signum, frame):
    logger.info("WebsiteCheck got interupted")
    sys.exit(0)

def check_websites():
    global lock_ws, lock_threads, thread_running, num_threads, websites
    lock_threads.acquire()
    num_threads += 1
    thread_running = True
    lock_threads.release()
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVER")
    security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL")
    ssl_cafile=os.getenv("KAFKA_SSL_CAFILE")
    ssl_certfile=os.getenv("KAFKA_SSL_CERTFILE")
    ssl_keyfile=os.getenv("KAFKA_SSL_KEYFILE")

    topic = os.getenv("WC_TOPIC")

    producer = KafkaProducer.WcKafkaProducer(bootstrap_servers,
                        security_protocol, ssl_cafile, ssl_certfile,
                        ssl_keyfile)

    producer.create_topic(topic)
    while True:
        lock_ws.acquire()
        for ws in websites:
            ws.get_response()
            producer.send(topic, ws.as_json().encode("utf-8"))
        lock_ws.release()
        producer.flush()
        time.sleep(10)

    lock_threads.acquire()
    num_threads -= 1
    lock_threads.release()

def read_websites():
    global lock_ws, lock_threads, thread_running, num_threads, websites
    lock_threads.acquire()
    num_threads += 1
    thread_running = True
    lock_threads.release()
    ws1 = website.Website("https://vpn.haas.works", regex="Werben")
    ws2 = website.Website("http://www.google.de", regex="Werben")
    lock_ws.acquire()
    websites.append(ws1)
    websites.append(ws2)
    lock_ws.release()
    lock_threads.acquire()
    num_threads -= 1
    lock_threads.release() 

def main():
    load_dotenv(verbose=True)
    loglevel = getattr(logging, os.getenv("LOGLEVEL").upper())
    if not isinstance(loglevel, int):
        print (f"{os.getenv('LOGLEVEL')} as LOGLEVEL is not valid. Using WARNING instead")
        loglevel = getattr(logging, "WARNING")
    logging.basicConfig(level=loglevel)

    signal.signal(signal.SIGINT, handler_int)
    signal.signal(signal.SIGTERM, handler_int)

    reader_thread = threading.Thread(target=read_websites)
    checker_thread = threading.Thread(target=check_websites)
    reader_thread.start()
    checker_thread.start()

main()