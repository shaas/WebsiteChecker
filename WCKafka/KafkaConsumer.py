import logging

from kafka import KafkaConsumer

logger = logging.getLogger(__name__)


class WcKafkaConsumer:
    def __init__(self, bootstrap_servers, security_protocol, ssl_cafile,
                 ssl_certfile, ssl_keyfile, topic):
        self.bootstrap_servers = bootstrap_servers
        self.security_protocol = security_protocol
        self.ssl_cafile = ssl_cafile
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile = ssl_keyfile
        self.__consumer = None
        self.topic = topic

    def __start_kafka_consumer(self):
        self.__consumer = KafkaConsumer(
            self.topic,
            auto_offset_reset="earliest",
            bootstrap_servers=self.bootstrap_servers,
            security_protocol=self.security_protocol,
            ssl_cafile=self.ssl_cafile,
            ssl_certfile=self.ssl_certfile,
            ssl_keyfile=self.ssl_keyfile,
            group_id="demo-group",
        )
        logger.info('KafkaConsumer started')

    def get_consumer(self):
        if not self.__consumer:
            self.__start_kafka_consumer()
        return self.__consumer
