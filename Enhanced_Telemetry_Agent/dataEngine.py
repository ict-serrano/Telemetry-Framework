import json
import time
import logging
import pymongo
import threading

from PyQt5.QtCore import QObject, pyqtSignal

logger = logging.getLogger("SERRANO.EnhancedTelemetryAgent.DataEngine")


class DataEngine(QObject):

    updatePMDS = pyqtSignal(object)

    def __init__(self, config):

        super(QObject, self).__init__()

        operational_db = config.get_operational_db()
        rest_interface = config.get_rest_interface()

        mongo_uri = "mongodb+srv://%s:%s@%s/?retryWrites=true&w=majority" % (operational_db["username"],
                                                                             operational_db["password"],
                                                                             operational_db["address"])
        mongo_client = pymongo.MongoClient(mongo_uri)

        self.__retain_period = config.get_retain_data_period()
        self.__agent_uuid = config.get_agent_uuid()
        self.__agent_url = rest_interface["exposed_service"]

        self.__lock = threading.Lock()
        self.__deployments_monitoring = {}

        self.__clusterCollection = mongo_client[operational_db["dbName"]]["clusters"]
        self.__entitiesCollection = mongo_client[operational_db["dbName"]]["entities"]
        self.__clusterMetricsCollection = mongo_client[operational_db["dbName"]]["cluster_state_metrics"]
        self.__edgeStorageCollection = mongo_client[operational_db["dbName"]]["edge_storage"]
        self.__edgeStorageMetricsCollection = mongo_client[operational_db["dbName"]]["edge_storage_metrics"]
        self.__deploymentsCollection = mongo_client[operational_db["dbName"]]["serrano_deployments"]
        self.__deploymentsSpecificMetricsCollection = mongo_client[operational_db["dbName"]]["deployments_specific_metrics"]

        self.__clusterDeploymentMetricsCollection = mongo_client[operational_db["dbName"]]["cluster_deployment_metrics"]

        self.__applicationCollection = mongo_client[operational_db["dbName"]]["application_metrics"]

        self.__initialize_agent_entity()
        self.__load_deployments_monitoring()

    def __initialize_agent_entity(self):

        if self.__entitiesCollection.count_documents({"uuid": self.__agent_uuid}) == 0:
            self.__entitiesCollection.insert_one({"uuid": self.__agent_uuid, "type": "Agent", "url": self.__agent_url,
                                                  "probes": [], "timestamp": int(time.time())})
        else:
            self.__entitiesCollection.update_one({"uuid": self.__agent_uuid}, {"$set": {"url": self.__agent_url,
                                                                                        "timestamp": int(time.time())}})

    def __load_deployments_monitoring(self):

        logger.info("Load existing deployments descriptions for monitoring ...")
        print("Load existing deployments descriptions for monitoring ...")

        try:
            agent_probes = self.__entitiesCollection.find_one({"uuid": self.__agent_uuid},
                                                              {"_id": 0, "probes": 1})["probes"]
            clusters = list(self.__entitiesCollection.find({"type": "Probe.k8s", "uuid": {"$in": agent_probes}},
                                                           {"_id": 0, "cluster_uuid": 1}))

            self.__lock.acquire()
            for c in clusters:
                for deployment in list(self.__deploymentsCollection.find({"clusters": {"$in": [c["cluster_uuid"]]}})):
                    self.__deployments_monitoring[deployment["deployment_uuid"]] = deployment[c["cluster_uuid"]]
            self.__lock.release()

            print(self.__deployments_monitoring)

        except Exception as e:
            logger.error(str(e))
            if self.__lock.locked():
                self.__lock.release()

    def get_agent_probes(self):
        ids = self.__entitiesCollection.find_one({"uuid": self.__agent_uuid})["probes"]
        return list(self.__entitiesCollection.find({"uuid": {"$in": ids}}))

    def handle_probe_registration_data(self, data):

        logger.info("Update operational database with registered probe '%s'" % data["probe_uuid"])
        logger.debug(json.dumps(data))

        if self.__entitiesCollection.count_documents({"uuid": data["probe_uuid"]}) == 0:
            self.__entitiesCollection.insert_one({"uuid": data["probe_uuid"], "type": data["type"], "url": data["url"],
                                                  "cluster_uuid": data["cluster_uuid"], "timestamp": int(time.time())})
        else:
            self.__entitiesCollection.update_one({"uuid": data["probe_uuid"]}, {"$set": {"url": data["url"],
                                                                                         "type": data["type"],
                                                                                         "cluster_uuid": data[
                                                                                             "cluster_uuid"],
                                                                                         "timestamp": int(
                                                                                             time.time())}})

        if self.__entitiesCollection.count_documents({"uuid": self.__agent_uuid,
                                                      "probes": {"$in": [data["probe_uuid"]]}}) == 0:
            self.__entitiesCollection.update_one({"uuid": self.__agent_uuid},
                                                 {"$push": {"probes": data["probe_uuid"]}})

        if data["type"] == "Probe.EdgeStorage":
            self.__set_edge_storage_probe_inventory_data(data)
        else:
            self.__set_cluster_probe_inventory_data(data)

    def __set_edge_storage_probe_inventory_data(self, data):
     
        for inventory_data in data["inventory"]["edge_storage_devices"]:
            if self.__edgeStorageCollection.count_documents({"name": inventory_data["name"],
                                                             "cluster_uuid": inventory_data["cluster_uuid"]}) == 0:
                inventory_data["timestamp"] = int(time.time())
                self.__edgeStorageCollection.insert_one(inventory_data)
            else:
                self.__edgeStorageCollection.update_one({"name": inventory_data["name"],
                                                         "cluster_uuid": inventory_data["cluster_uuid"]},
                                                        {"$set": {"timestamp": int(time.time()),
                                                                  "lat": inventory_data["lat"],
                                                                  "lng": inventory_data["lng"],
                                                                  "minio_node_disk_total_bytes": inventory_data["minio_node_disk_total_bytes"]}})

    def __set_cluster_probe_inventory_data(self, data):

        if self.__clusterCollection.count_documents({"uuid": data["cluster_uuid"]}) == 0:

            cluster_data = {"uuid": data["cluster_uuid"], "type": "k8s", "name": "", "timestamp": int(time.time())}

            if data["type"].find("Probe.k8s") != -1 or data["type"].find("Probe.K8s") != -1:
                cluster_data["type"] = "k8s"
                cluster_data["inventory"] = data["inventory"]["kubernetes_inventory_data"]
            else:
                cluster_data["type"] = "HPC"
                cluster_data["inventory"] = data["inventory"]

            self.__clusterCollection.insert_one(cluster_data)

        else:
            if data["type"].find("Probe.k8s") != -1 or data["type"].find("Probe.K8s") != -1:
                inventory_data = data["inventory"]["kubernetes_inventory_data"]
            else:
                inventory_data = data["inventory"]

            self.__clusterCollection.update_one({"uuid": data["cluster_uuid"]}, {"$set": {"timestamp": int(time.time()),
                                                                                          "inventory": inventory_data}})

    def handle_probe_deregistration_data(self, data):

        logger.info("Update operational database with registered probe '%s'" % data["probe_uuid"])
        logger.debug(json.dumps(data))

        entity = self.__entitiesCollection.find_one({"uuid": data["probe_uuid"]})

        # Delete inventory & monitoring data from operational db
        if entity["type"] == "Probe.EdgeStorage":
            self.__edgeStorageCollection.delete_many({"cluster_uuid": entity["cluster_uuid"]})
            self.__edgeStorageMetricsCollection.delete_many({"cluster_uuid": entity["cluster_uuid"]})
        else:
            self.__clusterCollection.delete_one({"uuid": entity["cluster_uuid"]})
            self.__clusterMetricsCollection.delete_many({"cluster_uuid": entity["cluster_uuid"]})

        # Delete the probe from ETA
        self.__entitiesCollection.update_one({"uuid": self.__agent_uuid}, {"$pull": {"probes": data["probe_uuid"]}})

        # Delete the probe entry
        self.__entitiesCollection.delete_one({"uuid": data["probe_uuid"]})

    def handle_application_monitoring(self, data):

        logger.info("Update list of application deployments for monitoring")
        logger.debug(json.dumps(data))

        try:

            # Handles the typical Pod metrics that are collected periodically by the Probe.K8s
            if data["action"] == "deployment":
                if data["request_method"] == "post":
                    self.__lock.acquire()
                    self.__deployments_monitoring[data["deployment_uuid"]] = data["k8s_deployments"]
                    self.__lock.release()
                elif data["request_method"] == "delete":
                    self.__lock.acquire()
                    if data["deployment_uuid"] in self.__deployments_monitoring:
                        del self.__deployments_monitoring[data["deployment_uuid"]]
                    self.__lock.release()

            # Handles the deployment/application specific metrics that are pushed by the users
            # through the simple sidecar mechanism
            if data["action"] == "deployment_specific_metrics":
                if data["request_method"] == "post":
                    self.__deploymentsSpecificMetricsCollection.delete_many({"deployment_uuid": data["metrics_data"]["deployment_uuid"],
                                                                             "timestamp": {"$lte": int(time.time()) - self.__retain_period}})

                    self.__deploymentsSpecificMetricsCollection.insert_one(data["metrics_data"])
                    self.updatePMDS.emit({"probe_type": "DeploymentSpecificMetrics", "data": data["metrics_data"]})

        except Exception as e:
            logger.error(str(e))
            if self.__lock.locked():
                self.__lock.release()

    def handle_probe_inventory_data(self, cluster_uuid, probe_uuid, probe_type, data):

        logger.info("Update operational database with inventory data from probe '%s'" % probe_uuid)
        logger.debug(json.dumps(data))

        if probe_type == "Probe.EdgeStorage":
            self.__set_edge_storage_probe_inventory_data({"inventory": data})
        else:
            self.__set_cluster_probe_inventory_data({"cluster_uuid": cluster_uuid,
                                                     "type": probe_type,
                                                     "inventory": data})

    def __extract_deployments_metrics(self, cluster_uuid, pods):
        data = []
        for p in list(filter(lambda pod: pod["serrano_deployment_uuid"] in self.__deployments_monitoring, pods)):
            p["timestamp"] = int(time.time())
            p["cluster_uuid"] = cluster_uuid
            p["deployment_uuid"] = p.pop("serrano_deployment_uuid")
            data.append(p)
        return data

    def handle_probe_monitoring_data(self, cluster_uuid, probe_uuid, probe_type, data):

        logger.info("Update operational database with operational data from probe '%s'" % probe_uuid)
        logger.debug(json.dumps(data))

        try:

            if probe_type == "Probe.EdgeStorage":

                # Delete "expired" metrics
                self.__edgeStorageMetricsCollection.delete_many({"cluster_uuid": cluster_uuid,
                                                                 "timestamp": {"$lte": int(time.time()) - self.__retain_period}})
                # Update edge storage metrics
                for d in data:
                    d["timestamp"] = int(time.time())
                    self.__edgeStorageMetricsCollection.insert_one(d)

            else:

                if probe_type == "Probe.k8s" and "Pods" in data:
                    deployments_metrics = self.__extract_deployments_metrics(cluster_uuid, data["Pods"])
                    logger.debug("Handle Deployment metrics ...")
                    if deployments_metrics:
                        self.__clusterDeploymentMetricsCollection.delete_many({"cluster_uuid": cluster_uuid,
                                                                               "timestamp": {"$lte": int(time.time()) - self.__retain_period }})
                        self.__clusterDeploymentMetricsCollection.insert_many(deployments_metrics)
                        self.updatePMDS.emit({"probe_type": "DeploymentMonitoring",
                                              "cluster_uuid": cluster_uuid,
                                              "probe_uuid": probe_uuid,
                                              "data": deployments_metrics})

                # Delete "expired" metrics
                self.__clusterMetricsCollection.delete_many({"cluster_uuid": cluster_uuid,
                                                             "timestamp": {"$lte": int(time.time()) - self.__retain_period}})

                # Update cluster_state_metrics
                self.__clusterMetricsCollection.insert_one({"cluster_uuid": cluster_uuid,
                                                            "timestamp": int(time.time()),
                                                            "state": data})

        except Exception as err:
            logger.error("Unable to update operational database")
            logger.error("%s - %s" % (err.__class__.__name__, str(err)))

        logger.info("Inform PMDSInterface")

        self.updatePMDS.emit({"probe_type": probe_type,
                              "cluster_uuid": cluster_uuid,
                              "probe_uuid": probe_uuid,
                              "data": data})


