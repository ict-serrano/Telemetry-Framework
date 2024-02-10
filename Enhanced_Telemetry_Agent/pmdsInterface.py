import json
import logging

from PyQt5.QtCore import QThread

from influxdb_client import InfluxDBClient, Point, Dialect, BucketRetentionRules
from influxdb_client.client.write_api import SYNCHRONOUS

logger = logging.getLogger("SERRANO.EnhancedTelemetryAgent.PMDSInterface")


class PMDSInterface(QThread):

    def __init__(self, config):

        QThread.__init__(self)

        influx_config = config.get_influxDB()
        self.__influx_org = influx_config["org"]

        client = InfluxDBClient(url="https://%s:%s" % (influx_config["address"], influx_config["port"]),
                                token=influx_config["token"],
                                org=influx_config["org"])

        self.__write_api = client.write_api(write_options=SYNCHRONOUS)
        self.__query_api = client.query_api()
        self.__buckets_api = client.buckets_api()

        self.__deployments_monitoring_bucket = "SERRANO_Deployments"
        self.__deployments_specific_metrics_bucket = "SERRANO_Deployments_Specific_Metrics"
        self.__grafana_deployments_monitoring_bucket = "SERRANO_Deployments_Metrics"

        self.__retention_rules = BucketRetentionRules(type="expire", every_seconds=315360000)

    def on_update_pmds(self, data):

        try:

            if data["probe_type"] == "Probe.k8s":
                self.__ensure_bucket(data["probe_uuid"])
                self.__handle_k8s_data(data["cluster_uuid"], data["probe_uuid"], data["data"])
            elif data["probe_type"] == "Probe.HPC":
                self.__ensure_bucket(data["probe_uuid"])
                self.__handle_hpc_data(data["cluster_uuid"], data["probe_uuid"], data["data"])
            elif data["probe_type"] == "Probe.EdgeStorage":
                self.__ensure_bucket(data["probe_uuid"])
                self.__handle_edge_storage_data(data["cluster_uuid"], data["probe_uuid"], data["data"])
            elif data["probe_type"] == "DeploymentMonitoring":
                self.__ensure_bucket(self.__deployments_monitoring_bucket)
                self.__ensure_bucket(self.__grafana_deployments_monitoring_bucket)
                self.__handle_deployment_monitoring_data(data["data"])
            elif data["probe_type"] == "DeploymentSpecificMetrics":
                self.__ensure_bucket(self.__deployments_specific_metrics_bucket)
                self.__handle_deployment_specific_metrics_data(data["data"])

        except Exception as err:
            print(err)
            logger.error("Unable to update PMDS for probe type '%s' - probe uuid '%s'" % (data["probe_type"],
                                                                                          data["probe_uuid"]))
            logger.error(str(err))

    def __ensure_bucket(self, probe_uuid):
        if len(self.__buckets_api.find_buckets(name=probe_uuid).buckets) == 0:
            logger.info("Create bucket for probe '%s'" % probe_uuid)
            self.__buckets_api.create_bucket(bucket_name=probe_uuid,
                                             retention_rules=self.__retention_rules,
                                             org=self.__influx_org)

    def __write_data_persistent_volumes(self, bucket_name, p_volumes):

        for pv in p_volumes:
            record = {"measurement": "persistentVolumes",
                      "tags": {"name": pv["name"]},
                      "fields": {"capacity_storage": pv["capacity"]["storage"]}}
            self.__write_api.write(bucket_name, self.__influx_org, record)

    def __write_data_pods(self, bucket_name, pods):

        for pod in pods:
            pod_tags = {"name": pod["name"], "namespace": pod["namespace"], "node": pod["node"],
                        "phase": pod["phase"], "creation_timestamp": pod["creation_timestamp"]}

            pod_fields = {"cpu_usage": pod["usage"]["cpu"], "memory_usage": pod["usage"]["memory"],
                          "restarts": pod["restarts"]}

            record = {"measurement": "pods", "tags": pod_tags, "fields": pod_fields}

            self.__write_api.write(bucket_name, self.__influx_org, record)

    def __write_data_deployments(self, bucket_name, deployments):

        for deployment in deployments:
            record = {"measurement": "deployments",
                      "tags": {"name": deployment["name"], "namespace": deployment["namespace"]},
                      "fields": {
                          "replicas": deployment["replicas"],
                          "ready_replicas": deployment["ready_replicas"],
                          "available_replicas": deployment["available_replicas"]}}

            self.__write_api.write(bucket_name, self.__influx_org, record)

    def __write_data_hpc_partitions(self, bucket_name, partitions, infrastructure_name):
        for partition in partitions:
            record = {"measurement": "hpc_partitions",
                      "tags": {"infrastructure_name": infrastructure_name, "partition_name": partition["name"]},
                      "fields": {
                          "avail_cpus": partition["avail_cpus"],
                          "avail_nodes": partition["avail_nodes"],
                          "queued_jobs": partition["queued_jobs"],
                          "running_jobs": partition["running_jobs"]
                      }}

            self.__write_api.write(bucket_name, self.__influx_org, record)

    def __write_data_edge_storage_device_data(self, bucket_name, data):
        for edge_data in data:
            record = {"measurement": "edge_storage",
                      "tags": {"cluster_uuid": edge_data["cluster_uuid"],
                               "node": edge_data["node"],
                               "name": edge_data["name"]},
                      "fields": {
                          "minio_bucket_usage_object_total": edge_data["minio_bucket_usage_object_total"],
                          "minio_bucket_usage_total_bytes": edge_data["minio_bucket_usage_total_bytes"],
                          "minio_node_disk_free_bytes": edge_data["minio_node_disk_free_bytes"],
                          "minio_node_disk_total_bytes": edge_data["minio_node_disk_total_bytes"],
                          "minio_node_disk_used_bytes": edge_data["minio_node_disk_used_bytes"],
                          "minio_s3_requests_total": edge_data["minio_s3_requests_total"]
                      }}
            self.__write_api.write(bucket_name, self.__influx_org, record)

    def __handle_edge_storage_data(self, cluster_uuid, probe_uuid, data):
        logger.info("Store edge storage devices data for cluster '%s' from probe '%s'" % (cluster_uuid, probe_uuid))
        self.__write_data_edge_storage_device_data(probe_uuid, data)

    def __handle_hpc_data(self, cluster_uuid, probe_uuid, data):
        logger.info("Store HPC monitoring data for cluster '%s' from probe '%s'" % (cluster_uuid, probe_uuid))

        if "partitions" in data and len(data["partitions"]) > 0:
            self.__write_data_hpc_partitions(probe_uuid, data["partitions"], data["name"])

    def __handle_k8s_data(self, cluster_uuid, probe_uuid, data):

        logger.info("Store K8s monitoring data for cluster '%s' from probe '%s'"%(cluster_uuid, probe_uuid))

        if "Nodes" in data:

            general = ["node_boot_time_seconds", "node_total_running_pods"]
            storage = ['node_filesystem_avail_bytes', 'node_filesystem_free_bytes', 'node_filesystem_size_bytes',
                       'node_filesystem_usage_percentage', 'node_filesystem_used_bytes']
            memory = ['node_memory_Buffers_bytes', 'node_memory_Cached_bytes', 'node_memory_MemAvailable_bytes',
                      'node_memory_MemFree_bytes', 'node_memory_MemTotal_bytes', 'node_memory_MemUsed_bytes',
                      'node_memory_usage_percentage', ]
            network = ['node_network_receive_bytes_total', 'node_network_receive_drop_total',
                       'node_network_receive_errs_total', 'node_network_receive_packets_total',
                       'node_network_transmit_bytes_total', 'node_network_transmit_drop_total',
                       'node_network_transmit_errs_total', 'node_network_transmit_packets_total']

            for node in data["Nodes"]:

                cpu_fields = {}
                general_fields = {x: node[x] for x in general}
                storage_fields = {x: node[x] for x in storage}
                memory_fields = {x: node[x] for x in memory}
                network_fields = {x: node[x] for x in network}

                for cpu in node["node_cpus"]:
                    cpu_fields["cpu_" + cpu["label"] + "_idle"] = cpu["idle"]
                    cpu_fields["cpu_" + cpu["label"] + "_used"] = cpu["used"]

                record = {"measurement": "nodes", "tags": {"node_name": node["node_name"], "group": "general"},
                          "fields": general_fields}
                self.__write_api.write(probe_uuid, self.__influx_org, record)
                record = {"measurement": "nodes", "tags": {"node_name": node["node_name"], "group": "cpu",
                                                           "node_cpus": len(node["node_cpus"])},
                          "fields": cpu_fields}
                self.__write_api.write(probe_uuid, self.__influx_org, record)
                record = {"measurement": "nodes", "tags": {"node_name": node["node_name"], "group": "memory"},
                          "fields": memory_fields}
                self.__write_api.write(probe_uuid, self.__influx_org, record)
                record = {"measurement": "nodes", "tags": {"node_name": node["node_name"], "group": "storage"},
                          "fields": storage_fields}
                self.__write_api.write(probe_uuid, self.__influx_org, record)
                record = {"measurement": "nodes", "tags": {"node_name": node["node_name"], "group": "network"},
                          "fields": network_fields}
                self.__write_api.write(probe_uuid, self.__influx_org, record)

        if "PersistentVolumes" in data:
            self.__write_data_persistent_volumes(probe_uuid, data["PersistentVolumes"])

        if "Pods" in data:
            self.__write_data_pods(probe_uuid, data["Pods"])

        if "Deployments" in data:
            self.__write_data_deployments(probe_uuid, data["Deployments"])

    def __handle_deployment_monitoring_data(self, data):

        for deployment_data in data:
            record = {"measurement": "serrano_deployments",
                      "tags": {"cluster_uuid": deployment_data["cluster_uuid"],
                               "node": deployment_data["node"],
                               "name": deployment_data["name"],
                               "deployment_uuid": deployment_data["deployment_uuid"],
                               "group_id": deployment_data["group_id"],
                               "namespace": deployment_data["namespace"]},
                      "fields": {"phase": deployment_data["phase"],
                                 "restarts": deployment_data["restarts"],
                                 "cpu_usage": deployment_data["usage"]["cpu"],
                                 "memory_usage": deployment_data["usage"]["memory"]}}

            memory_usage_mb = int(deployment_data["usage"]["memory"][:-2]) * 0.001024
            cpu_usage_m = int(deployment_data["usage"]["cpu"][:-1])/1000000

            grafana_record = {"measurement": "serrano_deployments",
                              "tags": {"cluster_uuid": deployment_data["cluster_uuid"],
                                       "node": deployment_data["node"],
                                       "name": deployment_data["name"],
                                       "deployment_uuid": deployment_data["deployment_uuid"],
                                       "group_id": deployment_data["group_id"]},
                              "fields": {"restarts": int(deployment_data["restarts"]),
                                         "cpu_usage_m": cpu_usage_m,
                                         "memory_usage_mb": memory_usage_mb}}

            self.__write_api.write(self.__deployments_monitoring_bucket, self.__influx_org, record)
            self.__write_api.write(self.__grafana_deployments_monitoring_bucket, self.__influx_org, grafana_record)

    def __handle_deployment_specific_metrics_data(self, data):

        record = {"measurement": "serrano_deployments_specific_metrics",
                  "tags": {"cluster_uuid": data["cluster_uuid"],
                           "deployment_uuid": data["deployment_uuid"],
                           "service_id": data["service_id"]},
                  "fields": data["metrics"]}

        self.__write_api.write(self.__deployments_specific_metrics_bucket, self.__influx_org, record)

    def __del__(self):
        self.wait()

    def run(self):
        logger.info("PMDSInterface is ready ...")
