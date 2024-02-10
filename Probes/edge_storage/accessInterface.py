import json
import logging

from flask import Flask
from flask import request
from flask import jsonify
from flask import make_response

from PyQt5.QtCore import QObject
from PyQt5.QtCore import QThread
from PyQt5.QtCore import pyqtSignal

from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash

logger = logging.getLogger('SERRANO.Probe.AccessInterface')


class RestAccessInterface(QThread):
    requestReceived = pyqtSignal(object)

    def __init__(self, config, probe):
        QThread.__init__(self)

        self.address = config["probe_interface"]["address"]
        self.port = config["probe_interface"]["port"]

        self.username = config["probe_interface"]["username"] if "username" in config["probe_interface"] else ""
        self.password = config["probe_interface"]["password"] if "password" in config["probe_interface"] else ""

        self.rest_app = Flask(__name__)

        self.monitor_config = {}

        logger.info("AccessInterface is ready.")

        auth = HTTPBasicAuth()

        @auth.verify_password
        def verify_password(username, password):
            if username == self.username and check_password_hash(self.password, password):
                return username

        @auth.error_handler
        def unauthorized():
            return make_response(jsonify({'error': 'Unauthorized access'}), 401)

        @self.rest_app.route("/api/v1/telemetry/probe/ping", methods=["GET"])
        @auth.login_required
        def ping():
            return make_response(jsonify({}), 200)

        @self.rest_app.route("/api/v1/telemetry/probe/inventory", methods=["GET"])
        @auth.login_required
        def edge_storage_inventory():
            params = request.args.to_dict()
            device_name = params.get("device_name")
            detect_devices = params.get("detect_devices")
            return make_response(jsonify(probe.get_inventory_data(device_name, detect_devices)), 200)

        @self.rest_app.route("/api/v1/telemetry/probe/monitor", methods=["GET"])
        @auth.login_required
        def edge_storage_monitoring():
            params = request.args.to_dict()
            device_name = params.get("device_name")
            detect_devices = params.get("detect_devices")
            return make_response(jsonify(probe.get_monitoring_data(device_name, detect_devices)), 200)

        @self.rest_app.route("/api/v1/telemetry/probe/collection", methods=["POST"])
        @auth.login_required
        def configure_data_collection():
            self.monitor_config = {}
            return make_response(jsonify({}), 200)

        @self.rest_app.route("/api/v1/telemetry/probe/streaming", methods=["POST"])
        @auth.login_required
        def activate_streaming_telemetry():
            return make_response(jsonify({}), 200)

        @self.rest_app.route("/api/v1/telemetry/probe/streaming/<uuid:session_uuid>", methods=["DELETE"])
        @auth.login_required
        def terminate_streaming_telemetry(session_uuid):
            return make_response(jsonify({}), 200)

    def __del__(self):
        self.wait()

    def run(self):
        logger.info("AccessInterface is running ...")
        self.rest_app.run(host=self.address, port=self.port, debug=False)
