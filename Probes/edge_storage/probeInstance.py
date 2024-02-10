import sys
import time
import yaml
import signal
import os.path
import logging
import requests

from PyQt5.QtCore import QCoreApplication

import accessInterface
import edgeStorageProbe

LOG_LEVEL = {"CRITICAL": 50, "ERROR": 40, "WARNING": 30, "INFO": 20, "DEBUG": 10}

CONF_FILE = "/etc/serrano/edge_storage_probe.yaml"
logger = logging.getLogger("SERRANO.TelemetryProbe.ProbeInstance")


class ProbeInstance:

    def __init__(self, conf):

        self.__config = conf

        self.__probe_uuid = conf["probe_uuid"]
        self.__cluster_uuid = conf["cluster_uuid"]

        self.probeInterface = None
        self.accessInterface = None

        logging.basicConfig(filename="%s.log" % (int(time.time())), level=LOG_LEVEL[self.__config["log_level"]])

        logger.info("Execution ...")

        self.probeInterface = edgeStorageProbe.EdgeStorageProbe(self.__probe_uuid,
                                                                self.__cluster_uuid,
                                                                self.__config["k8s"],
                                                                self.__config["edge_storage"])
        self.__probe_registration()

        self.accessInterface = accessInterface.RestAccessInterface(self.__config, self.probeInterface)
        self.accessInterface.start()

    def __probe_registration(self):

        agent_url = "https://%s:%s" % (self.__config["probe_interface"]["exposed_address"],
                                      self.__config["probe_interface"]["exposed_port"])

        request_url = "https://%s:%s" % (self.__config["telemetry_handler"]["address"],
                                        self.__config["telemetry_handler"]["port"])

        request_data = {"cluster_uuid": self.__cluster_uuid, "probe_uuid": self.__probe_uuid, "streaming_telemetry": 0,
                        "url": agent_url, "type": "Probe.EdgeStorage", "inventory": self.probeInterface.get_inventory_data()}

        try:

            response = requests.post("%s/api/v1/telemetry/agent/register" % request_url,
                                     headers={'Accept': 'application/json', 'Content-Type': 'application/json'},
                                     auth=(self.__config["telemetry_handler"]["username"],
                                           self.__config["telemetry_handler"]["password"]),
                                     json=request_data, verify=False, timeout=5)

            if response.status_code == 200 or response.status_code == 201:
                logger.info("Probe registered to telemetry controller: %s" % request_url)

        except Exception as err:
            logger.error("Unable to register probe to telemetry controller: %s" % request_url)
            logger.debug(str(err))


    def __probe_deregistration(self):
        pass


if __name__ == "__main__":

    config_params = None

    signal.signal(signal.SIGINT, signal.SIG_DFL)

    if os.path.exists(CONF_FILE):
        with open(CONF_FILE) as f:
            config_params = yaml.safe_load(f)

    if config_params is None:
        sys.exit(0)

    app = QCoreApplication(sys.argv)
    probe = ProbeInstance(config_params)
    sys.exit(app.exec_())

