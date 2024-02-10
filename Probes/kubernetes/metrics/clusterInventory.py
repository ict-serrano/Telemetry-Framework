import requests

from kubernetes import client
from prometheus_client.parser import text_string_to_metric_families


def k8s_cluster_inventory(api_client, node_exporter_endpoints):
    k8s_inventory_data = []

    api_core_client = client.CoreV1Api(api_client)

    for node in api_core_client.list_node().items:

        if "node-role.kubernetes.io/master" in node.metadata.labels or "node-role.kubernetes.io/control-plane" in node.metadata.labels:
            continue
        
        label = {}

        if "vaccel" in node.metadata.labels:
            label["vaccel"] = node.metadata.labels["vaccel"]
        else:
            label["vaccel"] = "false"
        
        if "security-tier" in node.metadata.labels:
            label["security-tier"] = node.metadata.labels["security-tier"]
        else:
            label["security-tier"] = 0

        data = {"node_role": "worker", "node_name": node.metadata.name, "node_annotations": [], "node_labels": label}

        capacity = node.status.capacity
        capacity["total_fpga"] = "0"
        capacity["total_gpu"] = "0"
        capacity["node_storage"] = "0"


        for k,v in capacity.items():
            if "nvidia.com/gpu" in k:
                capacity["total_gpu"] = str(int(capacity["total_gpu"])+int(v))
            if "xilinx.com/fpga-xilinx" in k:
                capacity["total_fpga"] = str(int(capacity["total_fpga"])+int(v))
        data["node_capacity"] = capacity
        data["node_info"] = node.status.node_info.to_dict()

        k8s_inventory_data.append(data)
        
    return {"kubernetes_inventory_data": k8s_inventory_data}

