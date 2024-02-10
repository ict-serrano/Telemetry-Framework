import json
import logging
import requests
import pymongo

from flask import Flask
from flask import request
from flask import jsonify
from flask import make_response

from PyQt5.QtCore import QThread
from PyQt5.QtCore import pyqtSignal

from flask_httpauth import HTTPBasicAuth
from werkzeug.security import check_password_hash

logger = logging.getLogger('SERRANO.EnhancedTelemetryAgent.AccessInterface')


class AccessInterface(QThread):

    restInterfaceMessage = pyqtSignal(object)

    def __init__(self, config):

        QThread.__init__(self)

        self.__config = config

        self.address = config.get_rest_interface()["address"]
        self.port = config.get_rest_interface()["port"]

        self.__registered_entities = {}
        self.__cache_k8s_inventory = {}

        self.rest_app = Flask(__name__)

        logger.info("AccessInterface is ready ...")

        auth = HTTPBasicAuth()

        @auth.verify_password
        def verify_password(username, password):
            return username

        @auth.error_handler
        def unauthorized():
            return make_response(jsonify({'error': 'Unauthorized access'}), 401)

        @self.rest_app.route("/api/v1/telemetry/agent/entities", methods=["GET"])
        @auth.login_required
        def get_registered_entities():
            data = []
            for entity_uuid, entity in self.__registered_entities.items():
                data.append({"probe_uuid": entity_uuid, "url": entity["url"],
                             "cluster_uuid": entity["cluster_uuid"],
                             "type": entity["type"]})
            return make_response(jsonify({"entities": data}), 200)

        @self.rest_app.route("/api/v1/telemetry/agent/register", methods=["POST"])
        @auth.login_required
        def entity_registration():
            data = request.get_json()
            self.__registered_entities[data["probe_uuid"]] = {"url": data["url"], "probe_uuid": data["probe_uuid"],
                                                              "type": data["type"],
                                                              "cluster_uuid": data["cluster_uuid"]}
            self.restInterfaceMessage.emit({"action": "registration", "request_params": data})
            return make_response(jsonify({}), 200)

        @self.rest_app.route("/api/v1/telemetry/agent/register/<uuid:entity_uuid>", methods=["GET", "PUT", "DELETE"])
        @auth.login_required
        def entity_management(entity_uuid):
            if request.method == "GET":
                if str(entity_uuid) in self.__registered_entities:
                    return make_response(jsonify(self.__registered_entities[str(entity_uuid)]), 200)
                else:
                    return make_response(jsonify({}), 404)
            elif request.method == "PUT":
                data = request.get_json()
                self.__registered_entities[str(entity_uuid)] = {"url": data["url"], "probe_uuid": data["probe_uuid"],
                                                                "cluster_uuid": data["cluster_uuid"],
                                                                "type": data["type"]}
                self.restInterfaceMessage.emit({"action": "registration", "request_params": data})
                return make_response(jsonify({}), 201)
            elif request.method == "DELETE":
                if str(entity_uuid) in self.__registered_entities:
                    del self.__registered_entities[str(entity_uuid)]
                self.restInterfaceMessage.emit({"action": "deregistration",
                                                "request_params": {"probe_uuid": str(entity_uuid)}})
                return make_response(jsonify({}), 200)

        @self.rest_app.route("/api/v1/telemetry/agent/streaming", methods=["POST"])
        @auth.login_required
        def streaming():
            data = {}
            return make_response(jsonify(data), 200)

        @self.rest_app.route("/api/v1/telemetry/agent/monitor/<uuid:entity_uuid>", methods=["GET"])
        @auth.login_required
        def monitor_entity(entity_uuid):
            data = {}
            if str(entity_uuid) not in self.__registered_entities:
                return make_response(jsonify({}), 404)

            q_url = "%s/api/v1/telemetry/probe/monitor" % (self.__registered_entities[str(entity_uuid)]["url"])
            target = request.args.to_dict().get("target", None)
            if target:
                q_url += "?target=%s" % target

            try:
                res = requests.get(q_url, verify=True)
                if res.status_code == 200 or res.status_code == 201:
                    monitor_data = json.loads(res.text)

                    evt_msg = {x: self.__registered_entities[str(entity_uuid)][x] for x in ["uuid", "type", "cluster_uuid"]}

                    evt_msg["action"] = "monitor"
                    evt_msg["monitoring_data"] = monitor_data

                    self.restInterfaceMessage.emit(evt_msg)
                    return make_response(jsonify(monitor_data), 200)
                else:
                    return make_response(jsonify({}), res.status_code)
            except Exception as err:
                logger.error("Unable to request monitoring data from entity '%s'" % entity_uuid)
                logger.error(str(err))
                return make_response(jsonify(data), 500)

        @self.rest_app.route("/api/v1/telemetry/agent/inventory/<uuid:entity_uuid>", methods=["GET"])
        @auth.login_required
        def inventory_entity(entity_uuid):
            data = {}
            if str(entity_uuid) not in self.__registered_entities:
                return make_response(jsonify({}), 404)
            try:
                res = requests.get(
                    "%s/api/v1/telemetry/probe/inventory" % (self.__registered_entities[str(entity_uuid)]["url"]), verify=True)

                if res.status_code == 200 or res.status_code == 201:
                    inventory_data = json.loads(res.text)

                    evt_msg = {x: self.__registered_entities[str(entity_uuid)][x] for x in ["uuid", "type", "cluster_uuid"]}
                    evt_msg["action"] = "inventory"
                    evt_msg["inventory_data"] = inventory_data

                    self.restInterfaceMessage.emit(evt_msg)
                    return make_response(jsonify(inventory_data), 200)
                else:
                    return make_response(jsonify({}), res.status_code)
            except Exception as err:
                logger.error("Unable to request inventory data from entity '%s'" % entity_uuid)
                logger.error(str(err))
                return make_response(jsonify(data), 500)

        @self.rest_app.route("/api/v1/telemetry/agent/deployments", methods=["POST"])
        @auth.login_required
        def post_serrano_deployment():
            data = request.get_json()
            self.restInterfaceMessage.emit({"action": "deployment",
                                            "request_method": "post",
                                            "deployment_uuid": data["deployment_uuid"],
                                            "k8s_deployments": data["k8s_deployments"]})
            return make_response(jsonify({}), 201)

        @self.rest_app.route("/api/v1/telemetry/agent/deployments/<uuid:deployment_uuid>", methods=["DELETE"])
        @auth.login_required
        def delete_serrano_deployment(deployment_uuid):
            self.restInterfaceMessage.emit({"action": "deployment",
                                            "request_method": "delete",
                                            "deployment_uuid": str(deployment_uuid)})
            return make_response(jsonify({}), 201)

        @self.rest_app.route("/api/v1/telemetry/agent/deployments/<uuid:deployment_uuid>", methods=["GET"])
        @auth.login_required
        def get_serrano_deployment(deployment_uuid):
            data = {}
            params = request.args.to_dict()
            if "deployment" in params:
                app = "deployment="+params["deployment"]
            else:
                app = None
            if str(entity_uuid) not in self.__registered_entities:
                return make_response(jsonify({}), 404)
            try:
                res = requests.get(
                    "%s/api/v1/telemetry/probe/application?%s" % (self.__registered_entities[str(entity_uuid)]["url"], app), verify=True)
                if res.status_code == 200 or res.status_code == 201:
                    application_data = json.loads(res.text)
                    self.restInterfaceMessage.emit({"action": "application", "application_data": application_data})
                    return make_response(jsonify(application_data), 200)
                else:
                    return make_response(jsonify({}), res.status_code)
            except Exception as err:
                logger.error("Unable to request application data from entity '%s'" % entity_uuid)
                logger.error(str(err))
                return make_response(jsonify(data), 500)

        @self.rest_app.route("/api/v1/telemetry/agent/deployment_specific_metrics", methods=["POST"])
        @auth.login_required
        def deployment_specific_metrics():
            self.restInterfaceMessage.emit({"action": "deployment_specific_metrics",
                                            "request_method": "post",
                                            "metrics_data": request.get_json()})
            return make_response(jsonify({}), 201)

        @self.rest_app.route("/api/v1/telemetry/agent", methods=["GET", "PUT"])
        @auth.login_required
        def configure_entity():
            if request.method == "GET":
                data = self.__config.get_rest_interface()
                data["query_interval"] = self.__config.get_query_interval()
                data["query_timeout"] = self.__config.get_query_timeout()
                data["active_monitoring"] = self.__config.get_active_monitoring()
                return make_response(jsonify(data), 200)
            elif request.method == "PUT":
                self.restInterfaceMessage.emit({"action": "configuration", "request_params": request.get_json()})
                return make_response(jsonify({}), 201)

    def set_registered_entities(self, entities):
        for entity in entities:
            self.__registered_entities[entity["uuid"]] = entity

    def __del__(self):
        self.wait()

    def run(self):
        logger.info("AccessInterface is running ...")
        self.rest_app.run(host=self.address, port=self.port, debug=False)


