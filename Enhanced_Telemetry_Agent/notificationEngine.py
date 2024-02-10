import json
import logging

import kafka
from kafka import KafkaProducer
from PyQt5.QtCore import QObject

logger = logging.getLogger("SERRANO.EnhancedTelemetryAgent.NotificationEngine")


class NotificationEngine(QObject):

    def __init__(self, config):
        super(QObject, self).__init__()

        logging.getLogger("kafka").setLevel(logging.WARNING)

        self.__kafka_notification_topic = "serrano_telemetry_notifications"
        self.__producer = KafkaProducer(bootstrap_servers=config["bootstrap_servers"],
                                        value_serializer=lambda v: json.dumps(v).encode("ascii"),
                                        compression_type='gzip')

    def on_telemetry_controller_event(self, event):
        logger.debug("Forward telemetry notification event ...")
        logger.debug(json.dumps(event))
        self.__producer.send(self.__kafka_notification_topic, event)
        self.__producer.flush()

    def on_analytic_engine_event(self, event):
        logger.debug("Forward telemetry notification event ...")
        logger.debug(json.dumps(event))
        self.__producer.send(self.__kafka_notification_topic, event)
        self.__producer.flush()
