import time
import logging
import requests
from kubernetes import client
from prometheus_client.parser import text_string_to_metric_families

logger = logging.getLogger('SERRANO.TelemetryProbe.K8sProbe')

def k8s_cluster_node_monitoring(node_exporter_data):
    data = {"node_cpus": []}

    try:
        for family in text_string_to_metric_families(node_exporter_data):

            if family.name == "node_boot_time_seconds":
                data[family.name] = family.samples[0].value

            if family.name == "node_cpu_seconds":
                for sample in family.samples:
                    if len(data["node_cpus"]) != int(sample.labels["cpu"]) + 1:
                        diff = (int(sample.labels["cpu"])+1) - len(data["node_cpus"])
                        for x in range(diff):
                            data["node_cpus"].append({"idle": 0, "used": 0})
                    data["node_cpus"][int(sample.labels["cpu"])]["label"] = sample.labels["cpu"]
                    if sample.labels["mode"] == "idle":
                        data["node_cpus"][int(sample.labels["cpu"])]["idle"] = sample.value
                    else:
                        data["node_cpus"][int(sample.labels["cpu"])]["used"] += sample.value

            if family.name in ["node_memory_Buffers_bytes", "node_memory_Cached_bytes", "node_memory_MemAvailable_bytes",
                               "node_memory_MemFree_bytes", "node_memory_MemTotal_bytes"]:
                data[family.name] = family.samples[0].value
                if "node_memory_MemTotal_bytes" in data and "node_memory_MemTotal_bytes" in data:
                    data["node_memory_MemUsed_bytes"] = data["node_memory_MemTotal_bytes"] - data[
                        "node_memory_MemFree_bytes"]
                    data["node_memory_usage_percentage"] = float(
                        "%.2f" % ((data["node_memory_MemUsed_bytes"] / data["node_memory_MemTotal_bytes"]) * 100))

            if family.name in ["node_filesystem_size_bytes", "node_filesystem_free_bytes", "node_filesystem_avail_bytes"]:
                for sample in family.samples:
                    if sample.labels["mountpoint"] == "/":
                        data[family.name] = sample.value

                if "node_filesystem_size_bytes" in data and "node_filesystem_free_bytes" in data:
                    data["node_filesystem_used_bytes"] = data["node_filesystem_size_bytes"] - data[
                        "node_filesystem_free_bytes"]
                    data["node_filesystem_usage_percentage"] = float(
                        "%.2f" % ((data["node_filesystem_used_bytes"] / data["node_filesystem_size_bytes"]) * 100))

            if family.name in ["node_network_receive_bytes", "node_network_receive_packets", "node_network_receive_drop",
                               "node_network_receive_errs"]:
                k = 0
                for sample in family.samples:
                    k += sample.value
                data[sample.name] = k

            if family.name in ["node_network_transmit_bytes", "node_network_transmit_packets", "node_network_transmit_drop",
                               "node_network_transmit_errs"]:
                k = 0
                for sample in family.samples:
                    k += sample.value
                data[sample.name] = k

    except Exception as err:
        logger.error("Unable to parse node-exporter response")
        logger.error(str(err))

    return data


def k8s_cluster_persistent_volumes_monitoring(api_client):
    data = []

    pvs = client.CoreV1Api(api_client).list_persistent_volume(watch=False)

    for item in pvs.items:
        pv = {}
        pv["name"] = item.metadata.name
        pv["creation_timestamp"] = item.metadata.creation_timestamp.timestamp()
        pv["capacity"] = item.spec.capacity
        data.append(pv)

    return data


def k8s_cluster_deployments_monitoring(api_client):
    data = []

    deployments = client.AppsV1Api(api_client).list_deployment_for_all_namespaces(watch=False)

    for deployment in deployments.items:
        dep = {}
        dep["name"] = deployment.metadata.name
        dep["namespace"] = deployment.metadata.namespace
        dep["creation_timestamp"] = deployment.metadata.creation_timestamp.timestamp()
        dep["replicas"] = deployment.status.replicas if deployment.status.replicas else 0
        dep["available_replicas"] = deployment.status.available_replicas if deployment.status.available_replicas else 0
        dep["ready_replicas"] = deployment.status.ready_replicas if deployment.status.ready_replicas else 0
        data.append(dep)

    return data


def k8s_cluster_pods_monitoring(api_client, cluster_worker_nodes):
    data = []

    pods = client.CoreV1Api(api_client).list_pod_for_all_namespaces(watch=False)

    for item in pods.items:
        if item.status.host_ip not in cluster_worker_nodes:
            continue

        pod = {"name": item.metadata.name, "namespace": item.metadata.namespace}
        pod["creation_timestamp"] = item.metadata.creation_timestamp.timestamp()
        pod["phase"] = item.status.phase       
        pod["node"] = cluster_worker_nodes[item.status.host_ip]
        pod["serrano_deployment_uuid"] = item.metadata.labels.get("serrano_deployment_uuid", "")
        pod["group_id"] = item.metadata.labels.get("group_id", "")
        pod["start_time"] = item.status.start_time.timestamp()
        pod["pod_ip"] = item.status.pod_ip
        pod["host_ip"] = item.status.host_ip
        pod["restarts"] = item.status.container_statuses[0].restart_count
        name = 'metadata.name=' + item.metadata.name
        top_pods = client.CustomObjectsApi(api_client).list_cluster_custom_object(group='metrics.k8s.io',
                                                                                  version='v1beta1',
                                                                                  plural='pods',
                                                                                  field_selector=name,
                                                                                  watch=False)
        if len(top_pods["items"]) == 0:
            continue

        pod["usage"] = top_pods["items"][0]['containers'][0]['usage']
        data.append(pod)

    return data


def k8s_cluster_services_monitoring(api_client):
    data = []

    services = client.CoreV1Api(api_client).list_service_for_all_namespaces(watch=False)

    for service in services.items:
        srv = {}
        srv["name"] = service.metadata.name
        srv["namespace"] = service.metadata.namespace
        srv["creation_timestamp"] = service.metadata.creation_timestamp.timestamp()
        srv["labels"] = service.metadata.labels
        srv["spec_type"] = service.spec.type
        data.append(srv)

    return data


def k8s_applications_monitoring(api_client, cluster_worker_nodes, namespaces):

    k8s_monitoring_data = {}

    k8s_monitoring_data["Deployments"] = k8s_cluster_deployments_monitoring(api_client)
    k8s_monitoring_data["Pods"] = k8s_cluster_pods_monitoring(api_client, cluster_worker_nodes)

    return k8s_monitoring_data


def k8s_deployment_monitoring(api_client):
    pass


def k8s_cluster_monitoring(api_client, node_exporter_endpoints, cluster_worker_nodes):
    count_pods = {}
    k8s_monitoring_data = {"Nodes": []}

    for node_host_ip, node_name in cluster_worker_nodes.items():
        count_pods[node_name] = 0

    pods = client.CoreV1Api(api_client).list_pod_for_all_namespaces(watch=False)

    for item in pods.items:
        if item.status.host_ip not in cluster_worker_nodes:
            continue

        if item.status.phase == "Running":
            count_pods[cluster_worker_nodes[item.status.host_ip]] += 1

    k8s_monitoring_data["PersistentVolumes"] = k8s_cluster_persistent_volumes_monitoring(api_client)

    for node_name, endpoint_ip in node_exporter_endpoints.items():
        try:
            logger.info("%s - %s" %(node_name, endpoint_ip))
            res = requests.get("http://%s:9100/metrics" % endpoint_ip, verify=False, timeout=5)
            logger.error(res.text)
            data = k8s_cluster_node_monitoring(res.text)
            data["node_name"] = node_name
            data["node_total_running_pods"] = count_pods[node_name]
            k8s_monitoring_data["Nodes"].append(data)
        except Exception as e:
            logger.error("Unable to monitor K8s resources")
            logger.error(str(e))
            #traceback.print_exc(file=sys.stdout)

    return k8s_monitoring_data

def k8s_application_data(api_client, deployment_name):#creates the list with metrics according to the parameters
    data = []
    deployments = client.AppsV1Api(api_client).list_deployment_for_all_namespaces(watch=False)
    pods = client.CoreV1Api(api_client).list_pod_for_all_namespaces(watch=False)
    
    for deployment in deployments.items:
        name = deployment.metadata.name
        message = deployment.status.conditions[0].message
        if "Deployment" in str(message):
            message = deployment.status.conditions[1].message
        if name == deployment_name and "ReplicaSet" in str(message):
            pod_name = message.split('"')
            for pod in pods.items:
                if (str(pod_name[1])+"-") in pod.metadata.name:
                    info = {"name": pod.metadata.name}
                    info["namespace"] = pod.metadata.namespace
                    info["node"] = pod.spec.node_name
                    info["creation_timestamp"] = pod.metadata.creation_timestamp.timestamp()
                    info["start_time"] = pod.status.start_time.timestamp()
                    info["phase"] = pod.status.phase
                    info["pod_ip"] = pod.status.pod_ip
                    info["host_ip"] = pod.status.host_ip
                    info["restarts"] = pod.status.container_statuses[0].restart_count
                    top_pods = client.CustomObjectsApi(api_client).list_cluster_custom_object(group='metrics.k8s.io',
                                                                                              version='v1beta1',
                                                                                              plural='pods',
                                                                                              field_selector='metadata.name=' + pod.metadata.name,
                                                                                              watch=False)
                    info["usage"] = top_pods["items"][0]['containers'][0]['usage']
                    data.append(info)
    return data

 
