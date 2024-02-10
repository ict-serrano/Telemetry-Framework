import json
import logging
import requests

from kubernetes import client
from prometheus_client.parser import text_string_to_metric_families

logger = logging.getLogger('SERRANO.Probe.EdgeStorageProbe')


class EdgeStorageMonitoring(object):

    def __init__(self):

        self.minio_node_process_uptime_seconds = 0
        self.minio_bucket_usage_object_total = 0
        self.minio_bucket_usage_total_bytes = 0
        self.minio_node_disk_total_bytes = 0
        self.minio_node_disk_free_bytes = 0
        self.minio_node_disk_used_bytes = 0
        self.minio_node_process_cpu_total_seconds = 0
        self.minio_node_process_resident_memory_bytes = 0
        self.minio_s3_requests_total = 0
        self.minio_s3_requests_errors_total = 0
        self.minio_s3_requests_waiting_total = 0
        self.minio_s3_requests_rejected_invalid_total = 0
        self.minio_s3_traffic_received_bytes = 0
        self.minio_s3_traffic_sent_bytes = 0

    def handle_samples(self, metric_name, samples):
        value = 0
        for sample in samples:
            value += sample.value
        setattr(self, metric_name, value)

    def to_dict(self):
        return self.__dict__


class EdgeStorageProbe:

    def __init__(self, probe_uuid, cluster_uuid, k8s_config, edge_storage_config):
        self.__k8s_config = k8s_config
        self.__edge_storage_config = edge_storage_config
        self.__cluster_uuid = cluster_uuid
        self.__probe_uuid = probe_uuid
        self.__probe_type = "Probe.EdgeStorage"

        self.__probe_config = {}
        self.__api_client = None
        self.__cluster_worker_nodes = {}
        self.__edge_storage_devices = {}

        self.__api_client_initialization()
        self.__detect_edge_storage_devices()

    def __api_client_initialization(self):
        api_configuration = client.Configuration()
        api_configuration.host = "https://%s:%s" % (self.__k8s_config["address"], self.__k8s_config["port"])
        api_configuration.verify_ssl = False
        api_configuration.api_key = {"authorization": "Bearer " + self.__k8s_config["token"]}

        self.__api_client = client.ApiClient(api_configuration)

        for node in client.CoreV1Api(self.__api_client).list_node().items:
            if "node-role.kubernetes.io/master" in node.metadata.labels or "node-role.kubernetes.io/control-plane" in node.metadata.labels:
                continue
            for address in node.status.addresses:
                if address.type == "InternalIP":
                    self.__cluster_worker_nodes[address.address] = node.metadata.name

    def __detect_edge_storage_devices(self):

        pods = client.CoreV1Api(self.__api_client).list_namespaced_pod(watch=False,
                                                                       namespace=self.__edge_storage_config["namespace"],
                                                                       label_selector="app=%s"%(self.__edge_storage_config["app_selector"]))

        for item in pods.items:
            self.__edge_storage_devices[item.metadata.name] = {"node": self.__cluster_worker_nodes[item.status.host_ip]}
            self.__edge_storage_devices[item.metadata.name]["url"] = "%s.edge-storage-devices" % item.metadata.name

    def __prometheus_samples_number(self, samples):
        value = 0
        for sample in samples:
            value += sample.value
        return value

    def __edge_storage_device_inventory(self, device_name):

        data = {}
        minio_node_disk_total_bytes = 0
        logger.debug("Query edge storage device '%s'" % device_name)

        try:
            res = requests.get("http://%s:7000/minio/v2/metrics/cluster" % self.__edge_storage_devices[device_name]["url"])
            if res.status_code == 200 or res.status_code == 201:
                for family in text_string_to_metric_families(res.text):
                    if family.name == "minio_node_disk_total_bytes":
                        minio_node_disk_total_bytes = self.__prometheus_samples_number(family.samples)

                data = {"name": device_name,
                        "node": self.__edge_storage_devices[device_name]["node"],
                        "cluster_uuid": self.__cluster_uuid,
                        "lat": self.__edge_storage_config["location"]["lat"],
                        "lng": self.__edge_storage_config["location"]["lng"],
                        "minio_node_disk_total_bytes": minio_node_disk_total_bytes}

            else:
                logger.error("Unable to retrieve inventory data for edge storage device '%s'" % device_name)

        except Exception as e:
            logger.error("Unable to retrieve inventory data for edge storage device '%s'" % device_name)
            logger.error(str(e))

        return data

    def __edge_storage_device_monitoring(self, device_name):
        data = {}
        monitoring = EdgeStorageMonitoring()
        logger.debug("Query edge storage device '%s'" % device_name)

        try:
            res = requests.get(
                "http://%s:7000/minio/v2/metrics/cluster" % self.__edge_storage_devices[device_name]["url"])
            if res.status_code == 200 or res.status_code == 201:
                for family in text_string_to_metric_families(res.text):
                    if hasattr(monitoring, family.name):
                        monitoring.handle_samples(family.name, family.samples)

                data = monitoring.to_dict()
                data["name"] = device_name
                data["node"] = self.__edge_storage_devices[device_name]["node"]
                data["cluster_uuid"] = self.__cluster_uuid
            else:
                logger.error("Unable to retrieve monitoring data for edge storage device '%s'" % device_name)

        except Exception as e:
            logger.error("Unable to retrieve monitoring data for edge storage device '%s'" % device_name)
            logger.error(str(e))

        return data

    def get_inventory_data(self, device_name=None, detect_edge_storage_devices=None):

        data = {"edge_storage_devices": []}

        if detect_edge_storage_devices:
            logger.info("Get the list of deployed edge storage devices within the K8s cluster")
            self.__detect_edge_storage_devices()

        if device_name:
            data["edge_storage_devices"].append(self.__edge_storage_device_inventory(device_name))
            return data

        logger.info("Retrieve the inventory data for all available edge storage devices")

        for device_name in self.__edge_storage_devices:
            data["edge_storage_devices"].append(self.__edge_storage_device_inventory(device_name))

        return data

    def get_monitoring_data(self, device_name=None, detect_edge_storage_devices=False):

        data = {"uuid": self.__probe_uuid, "type": self.__probe_type, "edge_storage_devices": []}

        if detect_edge_storage_devices:
            logger.info("Get the list of deployed edge storage devices within the K8s cluster")
            self.__detect_edge_storage_devices()

        if device_name:
            data["edge_storage_devices"].append(self.__edge_storage_device_monitoring(device_name))
            return data

        logger.info("Retrieve the monitoring data for all available edge storage devices")

        for device_name in self.__edge_storage_devices:
            data["edge_storage_devices"].append(self.__edge_storage_device_monitoring(device_name))

        return data
