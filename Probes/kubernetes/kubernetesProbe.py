import logging

from kubernetes import client

import metrics.clusterInventory as K8sInventory
import metrics.clusterMonitoring as K8sMonitoring

import metrics.Node_list as NodeListMetrics

logger = logging.getLogger("SERRANO.TelemetryProbe.KubernetesProbe")


class KubernetesProbe:

    def __init__(self, probe_uuid, cluster_uuid, k8s_config):
        self.__k8s_config = k8s_config
        self.__probe_config = {}
        self.__probe_uuid = probe_uuid
        self.__probe_type = "Probe.k8s"
        self.__api_client = None
        self.__cluster_worker_nodes = {}
        self.__node_exporter_endpoints = {}
        self.__cluster_uuid = cluster_uuid

        self.__api_client_initialization()

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

    def prometheus_node_exporter_endpoints(self, service_name, namespace):
        endpoint = client.CoreV1Api(self.__api_client).read_namespaced_endpoints(service_name, namespace)
        if len(endpoint.subsets) > 0:
            for addr in endpoint.subsets[0].addresses:
                if addr.node_name in self.__cluster_worker_nodes.values():
                    self.__node_exporter_endpoints[addr.node_name] = addr.ip

        print(self.__node_exporter_endpoints)

    def get_inventory_data(self):
        return K8sInventory.k8s_cluster_inventory(self.__api_client, self.__node_exporter_endpoints)

    def get_streaming_data(self):
        return NodeListMetrics.KubernetesMetrics().Stream()["kubernetes_stream_data"]

    def get_monitoring_data(self, params):

        data = {"uuid": self.__probe_uuid, "type": self.__probe_type, "kubernetes_monitoring_data": {}}

        target = "all"

        if "target" in params.keys() and params["target"] in ["resources", "applications"]:
            target = params["target"]

        if target == "all":
            d = K8sMonitoring.k8s_cluster_monitoring(self.__api_client,
                                                     self.__node_exporter_endpoints,
                                                     self.__cluster_worker_nodes)
            data["kubernetes_monitoring_data"].update(d)
            d = K8sMonitoring.k8s_applications_monitoring(self.__api_client, self.__cluster_worker_nodes, [])
            data["kubernetes_monitoring_data"].update(d)
        elif params["target"] == "resources":
            d = K8sMonitoring.k8s_cluster_monitoring(self.__api_client,
                                                     self.__node_exporter_endpoints,
                                                     self.__cluster_worker_nodes)
            data["kubernetes_monitoring_data"].update(d)
        elif params["target"] == "applications":
            d = K8sMonitoring.k8s_applications_monitoring(self.__api_client, self.__cluster_worker_nodes, [])
            data["kubernetes_monitoring_data"].update(d)

        return data

    def get_pods_info(self, deployment):
        if "deployment" in deployment:
            data = {"deployment_name": deployment["deployment"], "pod": {}, "probe_uuid": self.__probe_uuid, "cluster_uuid": self.__cluster_uuid}
            info = K8sMonitoring.k8s_application_data(self.__api_client, deployment["deployment"])
            data["pod"] = info
            return data
        else:
            return {'error': 'Bad request - check spelling deployment=deployment_name'}
    
    def set_probe_configuration(self, config):
        self.__probe_config = config
