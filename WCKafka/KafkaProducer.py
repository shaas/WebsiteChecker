import logging

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

logger = logging.getLogger(__name__)

class WcKafkaProducer:
    def __init__(self, bootstrap_servers, security_protocol, ssl_cafile, ssl_certfile, ssl_keyfile):
        self.bootstrap_servers = bootstrap_servers
        self.security_protocol = security_protocol
        self.ssl_cafile = ssl_cafile
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile = ssl_keyfile
        self.__producer = None

    def __start_producer(self):
        try:
            self.__producer = KafkaProducer(
                bootstrap_servers = self.bootstrap_servers,
                security_protocol = self.security_protocol,
                ssl_cafile = self.ssl_cafile,
                ssl_certfile = self.ssl_certfile,
                ssl_keyfile = self.ssl_keyfile,
                api_version = (2,6,0),
                acks="all",
            )
        except Exception as ex:
            logger.error("Could not start KafkaProducer: %s", str(ex))
        logger.info("KafkaProducer started")

    def send(self, topic, value):
        if not self.__producer:
            self.__start_producer()
        try:
            self.__producer.send(topic, value)
        except Exception as ex:
            logger.error("Kafka send failed: %s", str(ex))
        logger.debug("Kafka message sent")

    def flush(self):
        if not self.__producer:
            self.__start_producer()
        try:
            self.__producer.flush()
        except Exception as ex:
            logger.error("Kafka flush failed: %s", str(ex))
        logger.debug("Kafka Producer got flushed")

    def create_topic(self, topic):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers = self.bootstrap_servers,
                security_protocol = self.security_protocol,
                ssl_cafile = self.ssl_cafile,
                ssl_certfile = self.ssl_certfile,
                ssl_keyfile = self.ssl_keyfile,    
            )
            topic_list = []
            topic_list.append(NewTopic(topic, num_partitions=1, replication_factor=1))
            admin_client.create_topics(new_topics=topic_list,validate_only=False)
            admin_client.close()
        except Exception as ex:
            logger.error("Could not create topic %s: %s", topic, str(ex))