import json
import time
import logging

from PyQt5.QtCore import QTimer
from PyQt5.QtCore import QObject
from PyQt5.QtCore import pyqtSignal

logger = logging.getLogger("SERRANO.EnhancedTelemetryAgent.TelemetryController")


class TelemetryController(QObject):

    probesChanged = pyqtSignal(object)
    inventoryChanged = pyqtSignal(object)
    monitorChanged = pyqtSignal(object)
    notificationEvent = pyqtSignal(object)
    agentConfigurationChanged = pyqtSignal(object)
    applicationMonitoringChanged = pyqtSignal(object)

    def __init__(self):
        super(QObject, self).__init__()

    def handle_access_request(self, request):

        logger.info("Handle AccessInterface request.")
        logger.debug(request)

        if "action" not in request:
            logger.error("Invalid request description, discard it ...")
            return

        if request["action"] == "registration":
            event_msg = request["request_params"]
            event_msg["action"] = request["action"]
            self.probesChanged.emit(event_msg)
            self.notificationEvent.emit({"entity_id": request["request_params"]["probe_uuid"],
                                         "status": "UP",
                                         "type": "Probe",
                                         "timestamp": int(time.time())})

        elif request["action"] == "deregistration":
            event_msg = request["request_params"]
            event_msg["action"] = request["action"]
            self.probesChanged.emit(event_msg)
            self.notificationEvent.emit({"entity_id": request["request_params"]["probe_uuid"],
                                         "status": "DOWN",
                                         "type": "Probe",
                                         "timestamp": int(time.time())})

        elif request["action"] == "inventory":
            self.inventoryChanged.emit(request)

        elif request["action"] == "monitor":
            self.monitorChanged.emit(request)

        elif request["action"] == "configuration":
            self.agentConfigurationChanged.emit(request["request_params"])

        elif request["action"] in ["deployment", "deployment_specific_metrics"]:
            self.applicationMonitoringChanged.emit(request)

