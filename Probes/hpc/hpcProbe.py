import logging

import metrics.hpcInventory as HPCInventory
import metrics.hpcMonitoring as HPCMonitoring

logger = logging.getLogger("SERRANO.TelemetryProbe.HPCProbe")


class HPCProbe:

    def __init__(self, probe_uuid, hpc_config):
        self.__hpc_config = hpc_config
        self.__probe_config = {}
        self.__probe_uuid = probe_uuid
        self.__probe_type = "Probe.HPC"

    def get_inventory_data(self):
        return HPCInventory.hpc_inventory(self.__hpc_config)

    def get_monitoring_data(self, params):

        data = {"uuid": self.__probe_uuid,
                "type": self.__probe_type,
                "hpc_monitoring_data": HPCMonitoring.hpc_monitoring(self.__hpc_config)}

        return data

    def set_probe_configuration(self, config):
        self.__probe_config = config
