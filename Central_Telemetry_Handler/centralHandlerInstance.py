import sys
import yaml
import time
import signal
import os.path
import logging

from PyQt5.QtCore import QObject
from PyQt5.QtCore import QCoreApplication

import dataEngine
import dataCollector
import accessInterface
import telemetryController
import centralHandlerConfiguration

CONF_FILE = "/etc/serrano/central_handler.yaml"
LOG_LEVEL = {"CRITICAL": 50, "ERROR": 40, "WARNING": 30, "INFO": 20, "DEBUG": 10}

logger = logging.getLogger("SERRANO.CentralTelemetryHandler.CentralHandlerInstance")


class CentralHandlerInstance(QObject):

    def __init__(self, conf):
        super(QObject, self).__init__()

        self.config = centralHandlerConfiguration.CentralHandlerConfiguration(conf)

        self.dataEngine = None
        self.dataCollector = None
        self.accessInterface = None
        self.notificationEngine = None
        self.telemetryController = None

        logging.basicConfig(filename="%s.log" % (int(time.time())), level=LOG_LEVEL[self.config.get_log_level()])

    def boot(self):

        logger.info("Initialize services ... ")

        self.dataEngine = dataEngine.DataEngine(self.config)

        self.telemetryController = telemetryController.TelemetryController()

        self.accessInterface = accessInterface.AccessInterface(self.config, self.dataEngine)
        self.accessInterface.restInterfaceMessage.connect(self.telemetryController.handle_access_request)
        self.accessInterface.start()

        self.dataCollector = dataCollector.DataCollector(self.config, self.dataEngine)

        logger.info("SERRANO Central Telemetry Handler is ready ...")


if __name__ == "__main__":

    config_params = None

    signal.signal(signal.SIGINT, signal.SIG_DFL)

    if os.path.exists(CONF_FILE):
        with open(CONF_FILE) as f:
            config_params = yaml.safe_load(f)

    if config_params is None:
        sys.exit(0)

    app = QCoreApplication(sys.argv)

    instance = CentralHandlerInstance(config_params)
    instance.boot()

    sys.exit(app.exec_())

