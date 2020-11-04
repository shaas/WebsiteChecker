import logging

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

logger = logging.getLogger(__name__)


class WcKafkaProducer:
    """A Kafka Producer client that publishes records to the Kafka cluster.

    The producer is small abstraction of the original KafkaProducer which is
    limited to the features needed for WebsiteChecker.

    Keyword Arguments:
        bootstrap_servers: 'host[:port]' string (or list of strings)
        security_protocol (str): Protocol used to communicate with brokers.
        ssl_cafile (str): filename of ca file to use in certificate
            veriication.
        ssl_certfile (str): filename of file in pem format containing
            the client certificate.
        ssl_keyfile (str): filename containing the client private key.
    """
    def __init__(self, bootstrap_servers, security_protocol, ssl_cafile,
                 ssl_certfile, ssl_keyfile):
        self.bootstrap_servers = bootstrap_servers
        self.security_protocol = security_protocol
        self.ssl_cafile = ssl_cafile
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile = ssl_keyfile
        self.__producer = None

    def __start_producer(self):
        try:
            self.__producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                security_protocol=self.security_protocol,
                ssl_cafile=self.ssl_cafile,
                ssl_certfile=self.ssl_certfile,
                ssl_keyfile=self.ssl_keyfile,
                api_version=(2, 6, 0),
                acks="all",
            )
        except Exception as ex:
            logger.error("Could not start KafkaProducer: %s", str(ex))
        logger.info("KafkaProducer started")

    def send(self, topic, value):
        """ Publish a message to the Kafka cluster.

        Arguments:
            topic (str): Name of the topic where the message belongs to.
                NOTE: This topic must exist!
            value (byte): Message to publish to to Kafka cluster.
        """
        if not self.__producer:
            self.__start_producer()
        try:
            self.__producer.send(topic, value)
        except Exception as ex:
            logger.error("Kafka send failed: %s", str(ex))
        logger.debug("Kafka message sent")

    def flush(self):
        """ Invoking this method makes all buffered records immediately
        available to send.
        """
        if not self.__producer:
            self.__start_producer()
        try:
            self.__producer.flush()
        except Exception as ex:
            logger.error("Kafka flush failed: %s", str(ex))
        logger.debug("Kafka Producer got flushed")

    def create_topic(self, topic):
        """ Creates a topic at Kafka.

        Arguments:
            topic (str): Name of the topic to create
        """
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                security_protocol=self.security_protocol,
                ssl_cafile=self.ssl_cafile,
                ssl_certfile=self.ssl_certfile,
                ssl_keyfile=self.ssl_keyfile,
            )
            topic_list = []
            topic_list.append(NewTopic(topic, num_partitions=1,
                                       replication_factor=1))
            admin_client.create_topics(new_topics=topic_list,
                                       validate_only=False)
            admin_client.close()
        except Exception as ex:
            logger.error("Could not create topic %s: %s", topic, str(ex))
