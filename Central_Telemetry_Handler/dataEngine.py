import json
import time
import logging
import pymongo
import datetime
import requests

logger = logging.getLogger("SERRANO.CentralTelemetryHandler.DataEngine")


class DataEngine:

    def __init__(self, config):

        self.__storage_gateway_service = config.get_cloud_storage_locations()["address"]

        operational_db = config.get_operational_db()

        mongo_uri = "mongodb+srv://%s:%s@%s/?retryWrites=true&w=majority" % (operational_db["username"],
                                                                             operational_db["password"],
                                                                             operational_db["address"])

        mongo_client = pymongo.MongoClient(mongo_uri)

        self.__clusterCollection = mongo_client[operational_db["dbName"]]["clusters"]
        self.__kernelsCollection = mongo_client[operational_db["dbName"]]["serrano_kernels"]
        self.__kernelDeploymentsCollection = mongo_client[operational_db["dbName"]]["serrano_kernel_deployments"]
        self.__kernelMetricsCollection = mongo_client[operational_db["dbName"]]["serrano_kernel_metrics"]
        self.__entitiesCollection = mongo_client[operational_db["dbName"]]["entities"]
        self.__clusterMetricsCollection = mongo_client[operational_db["dbName"]]["cluster_state_metrics"]
        self.__cloudStorageCollection = mongo_client[operational_db["dbName"]]["cloud_storage_locations"]
        self.__edgeStorageCollection = mongo_client[operational_db["dbName"]]["edge_storage"]
        self.__edgeStorageMetricsCollection = mongo_client[operational_db["dbName"]]["edge_storage_metrics"]
        self.__infrastructureCollection = mongo_client[operational_db["dbName"]]["infrastructure"]
        self.__infrastructureMetricsCollection = mongo_client[operational_db["dbName"]]["serrano_state_metrics"]
        self.__deploymentsCollection = mongo_client[operational_db["dbName"]]["serrano_deployments"]
        self.__deploymentsSpecificMetricsCollection = mongo_client[operational_db["dbName"]]["deployments_specific_metrics"]

        self.__clusterDeploymentMetricsCollection = mongo_client[operational_db["dbName"]]["cluster_deployment_metrics"]
        self.__serranoTestbedCollection = mongo_client[operational_db["dbName"]]["serrano_testbed"]

        self.__applicationCollection = mongo_client[operational_db["dbName"]]["application_metrics"]

    def handle_cloud_storage_locations(self, data):

        logger.info("Update operational database with cloud storage locations")
        logger.debug(json.dumps(data))
        self.__cloudStorageCollection.delete_many({})
        self.__cloudStorageCollection.insert_many(data)


    def get_registered_agents(self):
        agents_by_cluster_id = {}

        for entity in self.__entitiesCollection.find({"type": "Agent"}, {"url": 1, "probes": 1}):
            for probe_id in entity["probes"]:
                c = self.__entitiesCollection.find_one({"uuid": probe_id}, {"cluster_uuid": 1})
                if c:
                    agents_by_cluster_id[c["cluster_uuid"]] = {"url": entity["url"], "probe_uuid": probe_id}

        return agents_by_cluster_id

    def get_infrastructure(self, args):
        data = {}
        query_kernels = args.get("kernels", None)

        data["k8s"] = list(self.__clusterCollection.find({"type": "k8s"},
                                                         {"_id": 0, "uuid": 1, "inventory":
                                                             {"node_info": {"architecture": 1},
                                                              "node_capacity": {
                                                                  "total_fpga": 1,
                                                                  "total_gpu": 1,
                                                                  "cpu": 1,
                                                                  "memory": 1},
                                                              "node_labels": 1}}))

        data["hpc"] = list(self.__clusterCollection.find({"type": "HPC"}, {"_id": 0,
                                                                           "uuid": 1,
                                                                           "inventory": {"partitions": 1}}))

        if not query_kernels:
            return data

        if len(query_kernels) == 0:
            query_kernels = "FaaS"

        filter_expression = {}
        if query_kernels.lower() == "faas":
            filter_expression["deployment_mode"] = "FaaS"
        if query_kernels.lower() == "standalone":
            filter_expression["deployment_mode"] = "Standalone"

        data["kernels"] = list(self.__kernelsCollection.find(filter_expression, {"_id": 0}))

        return data

    def get_infrastructure_inventory(self, uuid):
        data = {}
        self.uuid = uuid
        data["capacity"] = list(self.__clusterCollection.find({"uuid": self.uuid, }, {"_id": 0,
                                                                                      "inventory": {"node_name": 1,
                                                                                                    "node_capacity": {
                                                                                                        "total_fpga": 1,
                                                                                                        "total_gpu": 1,
                                                                                                        "cpu": 1,
                                                                                                        "memory": 1}}}))
        data["security"] = list(self.__clusterCollection.find({"uuid": self.uuid, },
                                                              {"_id": 0, "inventory": {"node_name": 1, "node_labels": {
                                                                  "vaccel": 1,
                                                                  "security-tier": 1}}}))
        return data

    def get_application_data(self, uuid):
        data = {}
        data = list(self.__applicationCollection.find({"cluster_uuid": uuid}, {"_id": 0}))
        return data

    def get_serrano_deployments(self, args):
        deployment_uuid = args.get("deployment_uuid", None)
        if deployment_uuid:
            return list(self.__deploymentsCollection.find({"deployment_uuid": deployment_uuid}, {"_id": 0}))
        else:
            return list(self.__deploymentsCollection.find({}, {"_id": 0,
                                                               "deployment_uuid": 1,
                                                               "clusters": 1,
                                                               "timestamp": 1}))

    def get_serrano_per_cluster_deployments(self, args):
        data = {}

        serrano_deployments = list(self.__deploymentsCollection.find({}, {"_id": 0}))

        for deployment in serrano_deployments:
            for cluster_uuid in deployment["clusters"]:
                if cluster_uuid not in data:
                    data[cluster_uuid] = []
                data[cluster_uuid] += deployment[cluster_uuid]

        return data

    def set_serrano_deployment(self, data):

        for cluster_uuid in data["clusters"]:
            probe_uuid = self.__entitiesCollection.find_one({"type": "Probe.k8s", "cluster_uuid": cluster_uuid},
                                                            {"uuid": 1})["uuid"]

            agent = self.__entitiesCollection.find_one({"type": "Agent", "probes": probe_uuid})

            logger.info("Request application monitoring from Agent '%s' @ '%s'" % (agent["uuid"], agent["url"]))

            try:
                res = requests.post(f"{agent['url']}/api/v1/telemetry/agent/deployments",
                                    json={"deployment_uuid": data["deployment_uuid"],
                                          "k8s_deployments": data[cluster_uuid]})
                if res.status_code != 201:
                    logger.error("Unable to forward application monitoring request to Agent '%s'" % agent["uuid"])
            except Exception as e:
                print(str(e))
                logger.error(str(e))

        if self.__deploymentsCollection.count_documents({"deployment_uuid": data["deployment_uuid"]}) > 0:
            self.__deploymentsCollection.delete_one({"deployment_uuid": data["deployment_uuid"]})
        self.__deploymentsCollection.insert_one(data)

    def delete_serrano_deployment(self, deployment_uuid):

        deployment = self.__deploymentsCollection.find_one({"deployment_uuid": deployment_uuid})

        if not deployment:
            return

        for cluster_uuid in deployment["clusters"]:
            probe_uuid = self.__entitiesCollection.find_one({"type": "Probe.k8s", "cluster_uuid": cluster_uuid},
                                                            {"uuid": 1})["uuid"]

            agent = self.__entitiesCollection.find_one({"type": "Agent", "probes": probe_uuid})

            logger.info("Terminate application monitoring from Agent '%s'" % agent["uuid"])

            try:
                res = requests.delete(f"{agent['url']}/api/v1/telemetry/agent/deployments/{deployment_uuid}")
                if res.status_code != 200 or res.status_code != 201:
                    logger.error(
                        "Unable to forward termination request for application monitoring to Agent '%s'" % agent[
                            "uuid"])
            except Exception as e:
                print(str(e))
                logger.error(str(e))

        self.__deploymentsCollection.delete_one({"deployment_uuid": deployment_uuid})

    def get_infrastructure_monitor(self, uuid):
        return {}

    def get_clusters(self):
        return list(self.__clusterCollection.find({}, {"_id": 0, "uuid": 1, "type": 1, "name": 1}))

    def get_cluster(self, cluster_uuid):
        return self.__clusterCollection.find_one({"uuid": cluster_uuid}, {"_id": 0})

    def get_cluster_metrics(self, cluster_uuid, args):

        if "target" in args and args["target"] == "all":
            return list(
                self.__clusterMetricsCollection.find({"cluster_uuid": cluster_uuid}, {"_id": 0}).sort("timestamp", -1))
        else:
            return list(
                self.__clusterMetricsCollection.find({"cluster_uuid": cluster_uuid}, {"_id": 0}).sort("timestamp",
                                                                                                      -1).limit(1))

    def get_storage_locations(self, args):
        data = {}
        target = args.get("target", None)

        simple_mapping = {}
        edge_ids_per_cluster = {}

        try:
            res = requests.get("http://%s/edge_locations" % self.__storage_gateway_service)
            for edge in res.json():
                device_cluster_uuid = simple_mapping.get(edge["cluster"], None)
                if device_cluster_uuid:
                    edge_ids_per_cluster[device_cluster_uuid][edge["region"]] = edge["id"]
        except Exception as e:
            print(str(e))
            logger.error(str(e))

        if not target or target == "edge":
            pipeline = [{"$project": {"_id": 0, "timestamp": 0}},
                        {"$lookup": {
                            "from": "edge_storage_metrics",
                            "localField": "name",
                            "foreignField": "name",
                            "as": "metrics",
                            "pipeline": [
                                {"$project": {"_id": 0,
                                              "minio_node_disk_total_bytes": 1,
                                              "minio_node_disk_used_bytes": 1,
                                              "minio_node_disk_free_bytes": 1}},
                                {"$limit": 1},
                                {"$sort": {"timestamp": -1}}
                            ]
                        }}]

            edge_devices = list(self.__edgeStorageCollection.aggregate(pipeline))

            def set_id(ed):
                ed["id"] = edge_ids_per_cluster[ed["cluster_uuid"]].get(ed["name"], -1)
                return ed

            data["edge_storage"] = list(map(set_id, edge_devices))

        if not target or target == "cloud":
            data["cloud_storage"] = list(self.__cloudStorageCollection.find({}, {"_id": 0}))

        return data

    def get_serrano_kernel_deployments(self, args):
        data = {}
        deployment_mode = args.get("deployment_mode", None)
        if deployment_mode:
            data["kernel_deployments"] = list(
                self.__kernelDeploymentsCollection.find({"deployment_mode": deployment_mode}, {"_id": 0}))
        else:
            data["kernel_deployments"] = list(self.__kernelDeploymentsCollection.find({}, {"_id": 0}))
        return data

    def update_serrano_kernel_deployments(self, data):
        if data["counter_diff"] < 0:
            r = self.__kernelDeploymentsCollection.find_one({"cluster_uuid": data["cluster_uuid"],
                                                             "deployment_mode": data["deployment_mode"]})
            if r and r.get(data["kernel_mode"], 0) == 0:
                return

        self.__kernelDeploymentsCollection.update_one({"deployment_mode": data["deployment_mode"],
                                                       "cluster_uuid": data["cluster_uuid"]},
                                                      {"$inc": {data["kernel_mode"]: data["counter_diff"]}})

    def add_serrano_kernel_metrics(self, data):
        self.__kernelMetricsCollection.insert_many(data["logs"])

    def get_serrano_kernel_metrics(self, args):

        query_filter = {}

        request_uuid = args.get("request_uuid", None)
        cluster_uuid = args.get("cluster_uuid", None)
        kernel_name = args.get("kernel_name", None)
        kernel_mode = args.get("kernel_mode", None)

        if request_uuid:
            query_filter["uuid"] = request_uuid
        if cluster_uuid:
            query_filter["cluster_uuid"] = cluster_uuid
        if kernel_name:
            query_filter["kernel_name"] = kernel_name
        if kernel_mode:
            query_filter["kernel_mode"] = kernel_mode

        return {"metrics": list(self.__kernelMetricsCollection.find(query_filter, {"_id": 0}))}

    def get_serrano_deployment_specific_metrics(self, deployment_uuid, args):
        query_filter = {"deployment_uuid": deployment_uuid}

        cluster_uuid = args.get("cluster_uuid", None)
        service_id = args.get("service_id", None)

        if cluster_uuid:
            query_filter["cluster_uuid"] = cluster_uuid
        if service_id:
            query_filter["service_id"] = service_id

        return {"specific_metrics": list(self.__deploymentsSpecificMetricsCollection.find(query_filter, {"_id": 0, "deployment_uuid":0}))}

 
