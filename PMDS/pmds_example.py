import requests

PMDS_SERVICE = ""

UVT_CLUSTER = ""
NBFC_CLUSTER = ""


"""
    Retrieve historical telemetry data for the worker nodes within a K8s cluster.
    
    Required parameters:
      - cluster_uuid (path parameter) => Determines the K8s cluster.
    
    The telemetry data for the available worker nodes within a K8s cluster are organized into five categories: 
    (1) general, (2) cpu, (3) memory, (4) storage and (5) network. The query parameter "group" can be used to 
    determine the group of metrics that the service will return. Supported values: "general", "cpu", "memory",
    "storage", "network". If not specified the "general" metrics will be returned. 
    
    Timeframe options:
     - start (query parameter) => Earliest time to include in results, by default is the last 24 hours (-1d). 
        Accepted formats are relative duration (e.g., -30m, -1h, -1d) or unix timestamp in seconds (e.g., 1644838147). 
        Durations are relative to now().
     - stop (query parameter) => Latest time to include in results. Default is now(). Accepted formats are relative 
        duration (e.g., -30m, -1h, -1d) or unix timestamp in seconds (e.g., 1644838147).Durations are relative to now().
    
    Filtering options:
      - node_name (query parameter) => Limits the results only for the pods that are running in the specified node name.   
      - field_measurement (query parameter) => Limits the results only for the selected parameter.  
      
    Formatting options: 
      - format (query parameter) => Determines the format of the response. Supported values "raw" and "compact".  
        The "raw" format provides the return data in a time series way. The "compact" format organizes the data at a 
        per persistent volume basis. If not specified the "compact" format will be used. 
"""


def pmds_service_query_nodes(cluster_uuid, **kwargs):
    valid_query_params = ["group", "start", "stop", "node_name", "field_measurement", "format"]

    query_params = {k: v for (k, v) in kwargs.items() if k in valid_query_params}

    res = requests.get(f"{PMDS_SERVICE}/api/v1/pmds/nodes/{cluster_uuid}", params=query_params)
    print(res.json())


"""
    Provide historical telemetry data for the available Deployments within a K8s cluster.
    
    Required parameters:
      - cluster_uuid (path parameter) => Determines the K8s cluster.
      - namespace (query parameter) =>  Determines the target namespace

    Parameters for requesting data for a specific timeframe, the same with the pmds_service_query_nodes()
    
    Filtering parameters:
      - name (path parameter) => Limits the results only for the deployment with the provided name.
    
    Response format parameter, the same with the pmds_service_query_nodes()
"""


def pmds_service_query_deployments(cluster_uuid, namespace, **kwargs):
    valid_query_params = ["start", "stop", "name", "format"]

    query_params = {k: v for (k, v) in kwargs.items() if k in valid_query_params}
    query_params["namespace"] = namespace

    res = requests.get(f"{PMDS_SERVICE}/api/v1/pmds/deployments/{cluster_uuid}", params=query_params)
    print(res.json())



"""
    Provide historical telemetry data for the available Pods within a K8s cluster.
    
    Required parameters:
      - cluster_uuid (path parameter) => Determines the K8s cluster.
      - namespace (query parameter) =>  Determines the target namespace

    Parameters for requesting data for a specific timeframe, the same with the pmds_service_query_nodes()
    
    Filtering parameters:
      - name (path parameter) => Limits the results only for the pod with the provided name.
      - node_name (path parameter) => Limits the results only for the pods that are running in the specified node name.  
    
    Response format parameter, the same with the pmds_service_query_nodes()
"""


def pmds_service_query_pods(cluster_uuid, namespace, **kwargs):
    valid_query_params = ["start", "stop", "name", "node_name", "format"]

    query_params = {k: v for (k, v) in kwargs.items() if k in valid_query_params}
    query_params["namespace"] = namespace

    res = requests.get(f"{PMDS_SERVICE}/api/v1/pmds/pods/{cluster_uuid}", params=query_params)
    data = res.json()

    if kwargs.get("format", "") == "raw":
        print("==> %s " % len(data))
        for d in data:
            print(d.keys())
    else:
        for k,v in data.items():
            if k.find("position-service") != -1:
                print("---> %s" % k)
                for i in v:
                    print(i)
                print("\n")

def pmds_service_query_edge_storage_devices(cluster_uuid, **kwargs):

        valid_query_params = ["start", "stop", "node_name", "name", "format"]

        query_params = {k: v for (k, v) in kwargs.items() if k in valid_query_params}

        res = requests.get(f"{PMDS_SERVICE}/api/v1/pmds/edge_storage_devices/{cluster_uuid}", params=query_params)
        data = res.json()

        print(data)

        for d in data["edge-storage-devices-0"]:
            print(d["time"])





def pmds_service_query_serrano_deployments(deployment_uuid, **kwargs):
    valid_query_params = ["start", "stop", "node_name", "cluster_uuid", "format"]

    query_params = {k: v for (k, v) in kwargs.items() if k in valid_query_params}

    res = requests.get(f"{PMDS_SERVICE}/api/v1/pmds/serrano_deployments/{deployment_uuid}", params=query_params)

    data = res.json()

    k = []
   
    import random

    #print(data)
    if kwargs.get("format", "") == "raw":
        #print("==> %s " % len(data))
        for d in data:
            if d["_field"] == "memory_usage":
                d["_value"] = (int(d["_value"][:-2])*0.001024)
                d["_value"] += random.uniform(-20,20)
                k.append(d) 
    else:
        for k,v in data.items():
            if k.find("position-service") != -1:
                print("---> %s" % k)
                for i in v:
                    print(i)
                print("\n")
    
    print(k)
#################
#   EXAMPLES    #
#################

pmds_service_query_nodes(UVT_CLUSTER, format="raw", start="-1h")

# Get collected data in "general" group for all nodes in the k8s cluster, for the last 24hours
pmds_service_query_nodes(UVT_CLUSTER, start="-24h")

# Get collected data in "memory" group for all nodes in the k8s cluster, for a specific timeframe
pmds_service_query_nodes(UVT_CLUSTER, group="memory", start="-4h", stop="-1h")

# Get collected data in "storage" group for all nodes in the k8s cluster, for the last 1 hour, using the raw format
pmds_service_query_nodes(UVT_CLUSTER, group="storage", start="-1h", format="raw")

# Get collected data in "cpu" group for worker node "serrano-k8s-worker-03" the last 6 hours
pmds_service_query_nodes(UVT_CLUSTER, group="cpu", node_name="serrano-k8s-worker-03", start="-6h")

# Get collected data in "cpu" group  using a specific timeframe
pmds_service_query_nodes(UVT_CLUSTER, group="cpu",  start="1676293139", stop="1676390944")

# Get collected data for specific metric in group 'memory' in all nodes and for specific timeframe
pmds_service_query_nodes(UVT_CLUSTER, group="memory", field_measurement="node_memory_MemUsed_bytes", start="-6h", stop="-30m")

# Get collected data for all deployments in "integration" namespace the last 8 hours
pmds_service_query_deployments(UVT_CLUSTER, "integration", start="-8h")

# Get collected data for all deployments in "integration" namespace the last 1 hour, using the raw format
pmds_service_query_deployments(UVT_CLUSTER, "integration", start="-1h", format="raw")

# Get collected data for all pods in "integration" namespace the last 12 hours
pmds_service_query_pods(UVT_CLUSTER, "integration", start="-12h")

# Get collected data for all pods in "integration" namespace that run on node "serrano-k8s-worker-01"
# for Monday 13/03 (start="1676239200" , stop="1676325599")
pmds_service_query_pods(UVT_CLUSTER, "integration", node_name="serrano-k8s-worker-01", start="1676239200", stop="1676325599")
































