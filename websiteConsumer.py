from dotenv import load_dotenv
from WCKafka import KafkaConsumer
from Database import postgre
from datetime import datetime

import logging
import os
import signal
import sys
import yaml

logger = logging.getLogger("WebsiteConsume")

database = None


def handler_int(signum, frame):
    logger.info("WebsiteConsume got interupted")
    if database:
        database.close_database()
    sys.exit(0)


def main():
    load_dotenv(verbose=True)
    loglevel = getattr(logging, os.getenv("LOGLEVEL").upper())
    if not isinstance(loglevel, int):
        print(f"{os.getenv('LOGLEVEL')} as LOGLEVEL is not valid. "
              "Using WARNING instead")
        loglevel = getattr(logging, "WARNING")
    logging.basicConfig(level=loglevel)

    signal.signal(signal.SIGINT, handler_int)
    signal.signal(signal.SIGTERM, handler_int)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVER")
    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL")
    ssl_cafile = os.getenv("KAFKA_SSL_CAFILE")
    ssl_certfile = os.getenv("KAFKA_SSL_CERTFILE")
    ssl_keyfile = os.getenv("KAFKA_SSL_KEYFILE")

    dbname = os.getenv("WC_DB_NAME")
    dbuser = os.getenv("WC_DB_USER")
    dbhost = os.getenv("WC_DB_HOST")
    dbport = os.getenv("WC_DB_PORT")
    dbpass = os.getenv("WC_DB_PASSWORD")

    topic = os.getenv("WC_TOPIC")

    database = postgre.Database(dbname, dbuser, dbhost, dbport, dbpass)
    database.open_database("WC", "WCEntries")
    consumer = KafkaConsumer.WcKafkaConsumer(bootstrap_servers,
                                             security_protocol, ssl_cafile,
                                             ssl_certfile, ssl_keyfile, topic,)

    for msg in consumer.get_consumer():
        entry = yaml.load(msg.value)
        logger.info("Received %s", entry)
        date = datetime.fromtimestamp(entry['date']['$date'] / 1e3)
        database.add_entry(entry['hash'], entry['url'],
                           date, entry['status'], entry['response_time'],
                           entry['regex_set'], entry['regex_found'])

    database.close_database()


main()
