import sys
import yaml
import time
import signal
import os.path
import logging

from PyQt5.QtCore import QObject
from PyQt5.QtCore import QCoreApplication

import dataEngine
import accessInterface
import PMDSConfiguration

CONF_FILE = "/etc/serrano/pmds.yaml"
LOG_LEVEL = {"CRITICAL": 50, "ERROR": 40, "WARNING": 30, "INFO": 20, "DEBUG": 10}

logger = logging.getLogger("SERRANO.PMDS.PMDSInstance")


class PMDSInstance(QObject):

    def __init__(self, conf):
        super(QObject, self).__init__()

        self.config = PMDSConfiguration.PMDSConfiguration(conf)

        self.dataEngine = None
        self.accessInterface = None

        logging.basicConfig(filename="%s.log" % (int(time.time())), level=LOG_LEVEL[self.config.get_log_level()])

    def boot(self):

        logger.info("Initialize services ... ")

        self.dataEngine = dataEngine.DataEngine(self.config.get_pmds_db())

        self.accessInterface = accessInterface.AccessInterface(self.config, self.dataEngine)
        self.accessInterface.start()

        logger.info("SERRANO PMDS is ready ...")


if __name__ == "__main__":

    config_params = None

    signal.signal(signal.SIGINT, signal.SIG_DFL)

    if os.path.exists(CONF_FILE):
        with open(CONF_FILE) as f:
            config_params = yaml.safe_load(f)

    if config_params is None:
        sys.exit(0)

    app = QCoreApplication(sys.argv)

    instance = PMDSInstance(config_params)
    instance.boot()

    sys.exit(app.exec_())

