import logging
import time

from flask import Flask
from flask import request
from flask import jsonify
from flask import make_response

from PyQt5.QtCore import QThread
from PyQt5.QtCore import pyqtSignal

from json import JSONEncoder
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import check_password_hash

logger = logging.getLogger('SERRANO.PMDS.AccessInterface')

class FluxStructure:
    pass

class FluxStructureEncoder(JSONEncoder):

    def default(self, obj):
        import datetime
        if isinstance(obj, FluxStructure):
            return obj.__dict__
        elif isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        return super().default(obj)

class AccessInterface(QThread):

    restInterfaceMessage = pyqtSignal(object)

    def __init__(self, config, data_engine):

        QThread.__init__(self)

        self.__config = config
        self.__dataEngine = data_engine

        self.address = self.__config.get_rest_interface()["address"]
        self.port = self.__config.get_rest_interface()["port"]

        self.rest_app = Flask(__name__)

        self.rest_app.json_encoder = FluxStructureEncoder

        self.__map_cluster_bucket = {}
     
        self.__map_cluster_edge_storage_bucket = {}
     
        auth = HTTPBasicAuth()

        logger.info("AccessInterface is ready ...")

        @auth.verify_password
        def verify_password(username, password):
            return username

        @auth.error_handler
        def unauthorized():
            return make_response(jsonify({'error': 'Unauthorized access'}), 401)

        @self.rest_app.route("/api/v1/pmds/pods/<uuid:cluster_uuid>", methods=["GET"])
        @auth.login_required
        def get_pods_metrics(cluster_uuid):

            args = request.args.to_dict()

            start = args.get("start", "-1d")
            bucket_uuid = self.__map_cluster_bucket.get(str(cluster_uuid), None)
            namespace = args.get("namespace", None)

            if not bucket_uuid:
                return make_response(jsonify({'error': 'Bad request - Unable to find specified cluster UUID'}), 400)

            if not namespace:
                return make_response(jsonify({'error': 'Bad request - Missing required namespace parameter'}), 400)

            params = {k: args[k] for k in set(list(args.keys()))-set(["start"])}

            return make_response(jsonify(self.__dataEngine.query_pods(bucket_uuid, start, **params)), 200)

        @self.rest_app.route("/api/v1/pmds/pvs/<uuid:cluster_uuid>", methods=["GET"])
        @auth.login_required
        def get_persistent_volumes_metrics(cluster_uuid):

            args = request.args.to_dict()
            start = args.get("start", "-1d")
            bucket_uuid = self.__map_cluster_bucket.get(str(cluster_uuid), None)

            if not bucket_uuid:
                return make_response(jsonify({'error': 'Bad request - Unable to find specified cluster UUID'}), 400)

            params = {k: args[k] for k in set(list(args.keys()))-set(["start"])}

            return make_response(jsonify(self.__dataEngine.query_persistent_volumes(bucket_uuid, start, **params)), 200)

        @self.rest_app.route("/api/v1/pmds/deployments/<uuid:cluster_uuid>", methods=["GET"])
        @auth.login_required
        def get_deployments_metrics(cluster_uuid):

            args = request.args.to_dict()

            start = args.get("start", "-1d")
            bucket_uuid = self.__map_cluster_bucket.get(str(cluster_uuid), None)
            namespace = args.get("namespace", None)

            if not bucket_uuid:
                return make_response(jsonify({'error': 'Bad request - Unable to find specified cluster UUID'}), 400)

            if not namespace:
                return make_response(jsonify({'error': 'Bad request - Missing required namespace parameter'}), 400)

            params = {k: args[k] for k in set(list(args.keys()))-set(["start"])}

            return make_response(jsonify(self.__dataEngine.query_deployments(bucket_uuid, start, **params)), 200)

        @self.rest_app.route("/api/v1/pmds/nodes/<uuid:cluster_uuid>", methods=["GET"])
        @auth.login_required
        def get_nodes_metrics(cluster_uuid):

            args = request.args.to_dict()

            start = args.get("start", "-1d")
            bucket_uuid = self.__map_cluster_bucket.get(str(cluster_uuid), None)

            if not bucket_uuid:
                return make_response(jsonify({'error': 'Bad request - Unable to find specified cluster UUID'}), 400)

            params = {k: args[k] for k in set(list(args.keys()))-set(["start"])}

            return make_response(jsonify(self.__dataEngine.query_nodes(bucket_uuid, start, **params)), 200)

        @self.rest_app.route("/api/v1/pmds/edge_storage_devices/<uuid:cluster_uuid>", methods=["GET"])
        @auth.login_required
        def get_edge_storage_devices_metrics(cluster_uuid):

            args = request.args.to_dict()

            start = args.get("start", "-1d")
            bucket_uuid = self.__map_cluster_edge_storage_bucket.get(str(cluster_uuid), None)

            if not bucket_uuid:
                return make_response(jsonify({'error': 'Bad request - Unable to find specified cluster UUID'}), 400)

            params = {k: args[k] for k in set(list(args.keys())) - set(["start"])}

            return make_response(jsonify(self.__dataEngine.query_edge_storage_devices(bucket_uuid, start, **params)), 200)

        @self.rest_app.route("/api/v1/pmds/serrano_deployments/<uuid:deployment_uuid>", methods=["GET"])
        @auth.login_required
        def get_serrano_deployments_metrics(deployment_uuid):
            bucket_uuid = "SERRANO_Deployments"

            args = request.args.to_dict()
            start = args.get("start", "-1d")
            params = {k: args[k] for k in set(list(args.keys())) - set(["start"])}

            return make_response(jsonify(self.__dataEngine.query_serrano_deployments(str(deployment_uuid),
                                                                                     bucket_uuid,
                                                                                     start,
                                                                                     **params)), 200)

    def __del__(self):
        self.wait()

    def run(self):
        logger.info("AccessInterface is running ...")
        self.rest_app.run(host=self.address, port=self.port, debug=False)
