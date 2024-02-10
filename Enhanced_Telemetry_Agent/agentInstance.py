import sys
import yaml
import time
import signal
import os.path
import logging

from PyQt5.QtCore import QObject
from PyQt5.QtCore import QCoreApplication

import pmdsInterface
import dataCollector
import accessInterface
import agentConfiguration
import notificationEngine
import telemetryController

CONF_FILE = "/etc/serrano/telemetry_agent.yaml"
LOG_LEVEL = {"CRITICAL": 50, "ERROR": 40, "WARNING": 30, "INFO": 20, "DEBUG": 10}

logger = logging.getLogger("SERRANO.EnhancedTelemetryAgent.AgentInstance")


class AgentInstance(QObject):

    def __init__(self, conf):
        super(QObject, self).__init__()

        self.config = agentConfiguration.AgentConfiguration(conf)

        self.pmdsInterface = None
        self.dataCollector = None
        self.accessInterface = None
        self.notificationEngine = None
        self.telemetryController = None

        logging.basicConfig(filename="%s.log" % (int(time.time())), level=LOG_LEVEL[self.config.get_log_level()])

    def boot(self):
        logger.info("Initialize services ... ")

        self.telemetryController = telemetryController.TelemetryController()

        self.accessInterface = accessInterface.AccessInterface(self.config)
        self.accessInterface.restInterfaceMessage.connect(self.telemetryController.handle_access_request)
        self.accessInterface.start()

        self.pmdsInterface = pmdsInterface.PMDSInterface(self.config)

        self.dataCollector = dataCollector.DataCollector(self.config)
        self.telemetryController.probesChanged.connect(self.dataCollector.on_probes_changed)
        self.telemetryController.inventoryChanged.connect(self.dataCollector.on_inventory_changed)
        self.telemetryController.monitorChanged.connect(self.dataCollector.on_monitor_changed)
        self.telemetryController.agentConfigurationChanged.connect(self.dataCollector.on_configuration_changed)
        self.telemetryController.applicationMonitoringChanged.connect(self.dataCollector.on_application_monitor)
                
        self.dataCollector.updatePMDS.connect(self.pmdsInterface.on_update_pmds)
        self.pmdsInterface.start()

        entities = self.dataCollector.on_boot_load_probes()
        self.accessInterface.set_registered_entities(entities)

        self.notificationEngine = notificationEngine.NotificationEngine(self.config["notification_engine"])
        self.telemetryController.notificationEvent.connect(self.notificationEngine.on_telemetry_controller_event)
        self.dataCollector.notificationEvent.connect(self.notificationEngine.on_telemetry_controller_event)


        logger.info("SERRANO Enhanced Telemetry Agent is ready ...")


if __name__ == "__main__":

    config_params = None

    signal.signal(signal.SIGINT, signal.SIG_DFL)

    if os.path.exists(CONF_FILE):
        with open(CONF_FILE) as f:
            config_params = yaml.safe_load(f)

    if config_params is None:
        sys.exit(0)

    app = QCoreApplication(sys.argv)

    instance = AgentInstance(config_params)
    instance.boot()

    sys.exit(app.exec_())

