import json
import time
import logging
import requests
import threading

import dataEngine

from PyQt5.QtCore import QObject, QTimer, pyqtSignal

logger = logging.getLogger("SERRANO.EnhancedTelemetryAgent.DataCollector")


class DataCollector(QObject):

    updatePMDS = pyqtSignal(object)
    notificationEvent = pyqtSignal(object)

    def __init__(self, config):

        super(QObject, self).__init__()

        self.__config = config

        self.__restProbes = {}
        self.__grpcProbes = {}
        self.__prometheusProbes = {}
        self.__flaggedProbes = []

        self.__active_application = True

        self.__query_interval = config.get_query_interval()
        self.__query_timeout = config.get_query_timeout()
        self.__active_monitoring = True

        self.__lock = threading.Lock()
        self.__dataEngine = dataEngine.DataEngine(config)
        self.__collectorTimer = None

        self.__dataEngine.updatePMDS.connect(self.__on_update_pmds)

        self.__setup_timer()

    def __setup_timer(self):
        if not self.__collectorTimer:
            self.__collectorTimer = QTimer(self)
            self.__collectorTimer.timeout.connect(self.__acquire_monitoring_data)
        else:
            self.__collectorTimer.stop()

        self.__collectorTimer.start(self.__query_interval * 1000)

    def __on_update_pmds(self, data):
        self.updatePMDS.emit(data)

    def on_configuration_changed(self, config_data):

        if "query_interval" in config_data:
            self.__query_interval = int(config_data["query_interval"])
            self.__config.set_query_interval(int(config_data["query_interval"]))
            self.__setup_timer()

        if "query_timeout" in config_data:
            self.__query_timeout = int(config_data["query_timeout"])
            self.__config.set_query_timeout(int(config_data["query_timeout"]))

        if "active_monitoring" in config_data:
            self.__config.set_active_monitoring(config_data["active_monitoring"])
            self.__active_monitoring = config_data["active_monitoring"]
        
        if "active_application" in config_data:
            self.__config.set_active_application(config_data["active_application"])
            self.__active_application = config_data["active_application"]

    def on_boot_load_probes(self):

        probes = []

        for probe in self.__dataEngine.get_agent_probes():
            try:
                requests.get("%s/api/v1/telemetry/probe/ping" % probe["url"],
                             verify=True,
                             timeout=self.__query_timeout)
                self.__lock.acquire()
                self.__restProbes[probe["uuid"]] = {"url": probe["url"],
                                                    "type": probe["type"],
                                                    "cluster_uuid": probe["cluster_uuid"]}
                probes.append(probe)
                self.__lock.release()
            except Exception as err:
                logger.error("Failed to query probe '%s'" % probe["uuid"])
                logger.error(str(err))

        return probes

    def on_probes_changed(self, event):

        logger.debug("Probes changed event: %s" % json.dumps(event))

        if "action" not in event:
            return

        if event["action"] == "onboot_load":
            print(event)

        if event["action"] == "registration":
            try:
                self.__lock.acquire()
                self.__restProbes[event["probe_uuid"]] = {"url": event["url"],
                                                          "type": event["type"],
                                                          "cluster_uuid": event["cluster_uuid"]}
                if event["probe_uuid"] in self.__flaggedProbes:
                    self.__flaggedProbes.remove(event["probe_uuid"])
                self.__dataEngine.handle_probe_registration_data(event)
            except Exception as err:
                logger.error("Failed to register probe")
                logger.error(str(err))
            finally:
                self.__lock.release()

        if event["action"] == "deregistration":

            if event["probe_uuid"] not in self.__restProbes.keys():
                return

            try:
                self.__lock.acquire()
                del self.__restProbes[event["probe_uuid"]]
                self.__dataEngine.handle_probe_deregistration_data(event)
            except Exception as err:
                logger.error("Failed to deregistrer probe")
                logger.error(str(err))
            finally:
                self.__lock.release()

    def on_inventory_changed(self, data):
        self.__dataEngine.handle_probe_inventory_data(data["cluster_uuid"],
                                                      data["uuid"],
                                                      data["type"],
                                                      data["inventory_data"])

    def on_monitor_changed(self, data):
        if data["type"] == "Probe.EdgeStorage":
            monitoring_data = data["monitoring_data"]["edge_storage_devices"]
        else:
            monitoring_data = data["monitoring_data"]
        self.__dataEngine.handle_probe_monitoring_data(data["cluster_uuid"],
                                                       data["uuid"],
                                                       data["type"],
                                                       monitoring_data)

    def on_application_monitor(self, data):
        self.__dataEngine.handle_application_monitoring(data)

    def __acquire_monitoring_data(self):
        self.__lock.acquire()
        probes = self.__restProbes
        self.__lock.release()

        if not self.__active_monitoring:
            return

        for probe_uuid, probe in probes.items():
            try:
                logger.info("Retrieve monitoring data from probe '%s'" % probe_uuid)
                res = requests.get("%s/api/v1/telemetry/probe/monitor" % probe["url"],
                                   verify=True,
                                   timeout=self.__query_timeout)
                if res.status_code == 200 or res.status_code == 201:
                    logger.debug(res.text)
                    data = json.loads(res.text)
                    if data["type"] == "Probe.k8s":
                        self.__dataEngine.handle_probe_monitoring_data(probe["cluster_uuid"],
                                                                       data["uuid"],
                                                                       data["type"],
                                                                       data["kubernetes_monitoring_data"])

                    if data["type"] == "Probe.HPC":
                        self.__dataEngine.handle_probe_monitoring_data(probe["cluster_uuid"],
                                                                       data["uuid"],
                                                                       data["type"],
                                                                       data["hpc_monitoring_data"])

                    if data["type"] == "Probe.EdgeStorage":
                        self.__dataEngine.handle_probe_monitoring_data(probe["cluster_uuid"],
                                                                       data["uuid"],
                                                                       data["type"],
                                                                       data["edge_storage_devices"])

            except Exception as err:
                if probe_uuid not in self.__flaggedProbes:
                    self.__flaggedProbes.append(probe_uuid)
                    self.notificationEvent.emit({"entity_id": probe_uuid, "status": "DOWN",
                                                 "type": "Probe", "timestamp": int(time.time())})

                logger.error("Unable to retrieve monitoring data from probe '%s'" % probe_uuid)
                logger.error(str(err))


