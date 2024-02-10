import json
import requests
import logging

logger = logging.getLogger("SERRANO.TelemetryProbe.HPCProbe")


def hpc_inventory(hpc_config):

    data = {"services":[], "partitions": [] }

    logger.info("Query SERRANO HPC Gateway for inventory information")

    try:

        res = requests.get("%s/services" % hpc_config["address"])

        if res.status_code == 200 or res.status_code == 201:
            data["services"] = json.loads(res.text)

        res = requests.get("%s/infrastructure/%s/telemetry" % (hpc_config["address"], hpc_config["infrastructure"]))

        if res.status_code == 200 or res.status_code == 201:
            d = json.loads(res.text)
            data["name"] = d["name"]
            data["scheduler"] = d["scheduler"]
            for partition in d["partitions"]:
                data["partitions"].append({"name": partition["name"],
                                           "total_nodes": partition["total_nodes"],
                                           "total_cpus": partition["total_cpus"]})

        logger.debug(data)

        return data

    except Exception as err:
        logger.error("Unable to query SERRANO HPC Gateway")
        logger.error(str(err))
        return {}
