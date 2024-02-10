import json
import pymongo
import logging
import requests

from flask import Flask
from flask import request
from flask import jsonify
from flask import make_response

from PyQt5.QtCore import QThread
from PyQt5.QtCore import pyqtSignal

from flask_httpauth import HTTPBasicAuth
from werkzeug.security import check_password_hash

logger = logging.getLogger('SERRANO.CentralTelemetryHandler.AccessInterface')


class AccessInterface(QThread):

    restInterfaceMessage = pyqtSignal(object)

    def __init__(self, config, data_engine):

        QThread.__init__(self)

        self.__config = config
        self.__dataEngine = data_engine
        self.__query_timeout = config.get_query_timeout()

        self.__cache_k8s_inventory = {}

        self.address = config.get_rest_interface()["address"]
        self.port = config.get_rest_interface()["port"]

        self.__agents_by_cluster_id = self.__dataEngine.get_registered_agents()

        self.rest_app = Flask(__name__)

        auth = HTTPBasicAuth()

        logger.info("AccessInterface is ready ...")

        @auth.verify_password
        def verify_password(username, password):
            return username

        @auth.error_handler
        def unauthorized():
            return make_response(jsonify({'error': 'Unauthorized access'}), 401)

        @self.rest_app.route("/api/v1/telemetry/central", methods=["GET", "PUT"])
        @auth.login_required
        def configure_central_handler():
            if request.method == "GET":
                data = self.__config.get_rest_interface()
                data["query_interval"] = self.__config.get_query_interval()
                data["query_timeout"] = self.__config.get_query_timeout()
                data["active_monitoring"] = self.__config.get_active_monitoring()
                return make_response(jsonify(data), 200)
            elif request.method == "PUT":
                self.restInterfaceMessage.emit({"action": "configuration", "request_params": request.get_json()})
                return make_response(jsonify({}), 201)

        @self.rest_app.route("/api/v1/telemetry/central/infrastructure", methods=["GET"])
        @auth.login_required
        def get_infrastructure():
            return make_response(jsonify(self.__dataEngine.get_infrastructure(request.args.to_dict())), 200)

        @self.rest_app.route("/api/v1/telemetry/central/infrastructure/inventory/<uuid:cluster_uuid>", methods=["GET"])#/<uuid>
        @auth.login_required
        def get_infrastructure_inventory(cluster_uuid):
            return make_response(jsonify(self.__dataEngine.get_infrastructure_inventory(str(cluster_uuid))), 200)

        @self.rest_app.route("/api/v1/telemetry/central/infrastructure/monitor/<uuid:cluster_uuid>", methods=["GET"])#/<uuid>
        @auth.login_required
        def get_infrastructure_monitor(cluster_uuid):
            return make_response(jsonify(self.__dataEngine.get_infrastructure_monitor(str(cluster_uuid))), 200)

        @self.rest_app.route("/api/v1/telemetry/central/storage_locations", methods=["GET"])
        @auth.login_required
        def get_storage_locations():
            return make_response(jsonify(self.__dataEngine.get_storage_locations(request.args.to_dict())), 200)

        @self.rest_app.route("/api/v1/telemetry/central/deployments", methods=["GET"])
        @auth.login_required
        def get_deployments():
            args = request.args.to_dict()
            return make_response(jsonify({"deployments": self.__dataEngine.get_serrano_deployments(args)}), 200)

        @self.rest_app.route("/api/v1/telemetry/central/cluster_deployments", methods=["GET"]) # New for final DEMOS
        @auth.login_required
        def get_per_cluster_deployments():
            args = request.args.to_dict()
            return make_response(jsonify({"cluster_deployments": self.__dataEngine.get_serrano_per_cluster_deployments(args)}), 200)

        @self.rest_app.route("/api/v1/telemetry/central/deployments", methods=["POST"])
        @auth.login_required
        def set_serrano_deployment():
            self.__dataEngine.set_serrano_deployment(request.get_json())
            return make_response(jsonify({}), 201)

        @self.rest_app.route("/api/v1/telemetry/central/deployments/<uuid:deployment_uuid>", methods=["GET", "DELETE"])
        @auth.login_required
        def serrano_deployment(deployment_uuid):
            if request.method == "GET":
                data = {"deployments": self.__dataEngine.get_serrano_deployments({"deployment_uuid": str(deployment_uuid)})}
                return make_response(jsonify(data), 200)
            elif request.method == "DELETE":
                return make_response(jsonify(self.__dataEngine.delete_serrano_deployment(str(deployment_uuid))), 200)

        @self.rest_app.route("/api/v1/telemetry/central/clusters", methods=["GET"])
        @auth.login_required
        def get_available_clusters():
            return make_response(jsonify({"clusters": self.__dataEngine.get_clusters()}), 200)

        @self.rest_app.route("/api/v1/telemetry/central/clusters/<uuid:cluster_uuid>", methods=["GET"])
        @auth.login_required
        def cluster_inventory_from_db(cluster_uuid):
            cluster_uuid = str(cluster_uuid)
            if cluster_uuid in self.__dataEngine.get_registered_agents().keys():
                return make_response(jsonify(self.__dataEngine.get_cluster(str(cluster_uuid))), 200)
            else:
                return make_response(jsonify({}), 404)

        @self.rest_app.route("/api/v1/telemetry/central/clusters/inventory/<uuid:cluster_uuid>", methods=["GET"])
        @auth.login_required
        def cluster_inventory(cluster_uuid):
            cluster_uuid = str(cluster_uuid)
            agents_by_cluster_id = self.__dataEngine.get_registered_agents()
            if cluster_uuid in agents_by_cluster_id.keys():
                agent_url = agents_by_cluster_id[cluster_uuid]["url"]
                probe_uuid = agents_by_cluster_id[cluster_uuid]["probe_uuid"]
                try:
                    res = requests.get("%s/api/v1/telemetry/agent/inventory/%s" % (agent_url, probe_uuid),
                                       verify=True,
                                       timeout=self.__query_timeout)
                    if res.status_code == 200 or res.status_code == 201:
                        return make_response(jsonify(json.loads(res.text)), 200)
                    else:
                        return make_response(jsonify({}), res.status_code)
                except Exception as e:
                    print(str(e))
                    return make_response(jsonify({}), 404)

            return make_response(jsonify({}), 404)

        @self.rest_app.route("/api/v1/telemetry/central/clusters/monitor/<uuid:cluster_uuid>", methods=["GET"])
        @auth.login_required
        def cluster_monitor(cluster_uuid):
            cluster_uuid = str(cluster_uuid)
            agents_by_cluster_id = self.__dataEngine.get_registered_agents()
            if cluster_uuid in agents_by_cluster_id.keys():
                agent_url = agents_by_cluster_id[cluster_uuid]["url"]
                probe_uuid = agents_by_cluster_id[cluster_uuid]["probe_uuid"]
                q_url = "%s/api/v1/telemetry/agent/monitor/%s" % (agent_url, probe_uuid)
                target = request.args.to_dict().get("target", None)
                if target:
                    q_url += "?target=%s" % target

                try:
                    res = requests.get(q_url, verify=True, timeout=self.__query_timeout)
                    if res.status_code == 200 or res.status_code == 201:
                        return make_response(jsonify(json.loads(res.text)), 200)
                    else:
                        return make_response(jsonify({}), res.status_code)
                except Exception as e:
                    print(e)
                    return make_response(jsonify({}), 404)

            return make_response(jsonify({}), 404)

        @self.rest_app.route("/api/v1/telemetry/central/clusters/metrics/<uuid:cluster_uuid>", methods=["GET"])
        @auth.login_required
        def cluster_metrics(cluster_uuid):
            cluster_uuid = str(cluster_uuid)
            if cluster_uuid in self.__dataEngine.get_registered_agents().keys():
                data = self.__dataEngine.get_cluster_metrics(cluster_uuid, request.args.to_dict())
                return make_response(jsonify({"metrics": data}), 200)

            return make_response(jsonify({}), 404)

        @self.rest_app.route("/api/v1/telemetry/central/serrano_kernel_deployments", methods=["GET", "PUT"])
        @auth.login_required
        def serrano_kernel_deployments():
            if request.method == "GET":
                data = self.__dataEngine.get_serrano_kernel_deployments(request.args.to_dict())
                return make_response(jsonify(data), 200)
            elif request.method == "PUT":
                self.__dataEngine.update_serrano_kernel_deployments(request.get_json())
                return make_response(jsonify({}), 201)
            else:
                return make_response(jsonify({}), 404)

        @self.rest_app.route("/api/v1/telemetry/central/kernel_metrics", methods=["GET", "POST"])
        @auth.login_required
        def serrano_kernel_metrics():
            if request.method == "POST":
                self.__dataEngine.add_serrano_kernel_metrics(request.get_json())
                return make_response(jsonify({}), 200)
            else:
                return make_response(jsonify(self.__dataEngine.get_serrano_kernel_metrics(request.args.to_dict())), 200)

        @self.rest_app.route("/api/v1/telemetry/central/deployment_specific_metrics/<uuid:deployment_uuid>", methods=["GET"])
        @auth.login_required
        def serrano_deployments_custom_metrics(deployment_uuid):
            deployment_uuid = str(deployment_uuid)
            metrics = self.__dataEngine.get_serrano_deployment_specific_metrics(deployment_uuid, request.args.to_dict())
            return make_response(jsonify(metrics), 200)

        @self.rest_app.route("/api/v1/telemetry/central/deployment_metrics/<uuid:deployment_uuid>", methods=["GET"])
        @auth.login_required
        def serrano_deployments_metrics(deployment_uuid):
            deployment_uuid = str(deployment_uuid)
            return make_response(jsonify({"metrics": self.__dataEngine.get_serrano_deployment_metrics(deployment_uuid)}), 200)

    def __del__(self):
        self.wait()

    def run(self):
        logger.info("AccessInterface is running ...")
        self.rest_app.run(host=self.address, port=self.port, debug=False)
