import json
import requests
import logging

logger = logging.getLogger("SERRANO.TelemetryProbe.HPCProbe")


def hpc_monitoring(hpc_config):

    data = {"partitions": []}

    logger.info("Query SERRANO HPC Gateway for monitoring information")

    try:
        res = requests.get("%s/infrastructure/%s/telemetry" % (hpc_config["address"], hpc_config["infrastructure"]))

        if res.status_code == 200 or res.status_code == 201:
            d = json.loads(res.text)
            data["name"] = d["name"]
            data["scheduler"] = d["scheduler"]
            data["partitions"] = d["partitions"]

        logger.debug(data)

        return data

    except Exception as err:
        logger.error("Unable to query SERRANO HPC Gateway")
        logger.error(str(err))
        return {}
