import json
import time
import logging
import requests
#import threading

from PyQt5.QtCore import QObject, QTimer, pyqtSignal

logger = logging.getLogger("SERRANO.CentralTelemetryHandler.DataCollector")


class DataCollector(QObject):

    notificationEvent = pyqtSignal(object)

    def __init__(self, config, dataEngine):

        super(QObject, self).__init__()

        self.__config = config
        self.__query_interval = config.get_query_interval()
        self.__query_timeout = config.get_query_timeout()

        self.__dataEngine = dataEngine
        self.__collectorTimer = None

        self.__query_cloud_storage_locations(config.get_cloud_storage_locations())
        self.__setup_timer()

    def __setup_timer(self):
        if not self.__collectorTimer:
            self.__collectorTimer = QTimer(self)
            self.__collectorTimer.timeout.connect(self.__acquire_platform_level_data)
        else:
            self.__collectorTimer.stop()

        self.__collectorTimer.start(self.__query_interval * 1000)

    def __query_cloud_storage_locations(self, params):
        try:
            res = requests.get("https://%s/cloud_locations" % (params["address"]), verify=True)
            if res.status_code == 200 or res.status_code == 201:
                self.__dataEngine.handle_cloud_storage_locations(json.loads(res.text))
            else:
                logger.error("Unable to query cloud storage locations")
        except Exception as err:
            logger.error("Unable to query cloud storage locations")
            logger.error(str(err))


