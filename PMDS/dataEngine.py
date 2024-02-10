import logging

from typing import List
from json import JSONEncoder
from influxdb_client import InfluxDBClient, Point, Dialect, BucketRetentionRules
from influxdb_client.client.flux_table import TableList

logger = logging.getLogger("SERRANO.PMDS.DataEngine")

NODES_GENERAL = ["node_boot_time_seconds", "node_total_running_pods"]
NODES_STORAGE = ['node_filesystem_avail_bytes', 'node_filesystem_free_bytes', 'node_filesystem_size_bytes',
                 'node_filesystem_usage_percentage', 'node_filesystem_used_bytes']
NODES_MEMORY = ['node_memory_Buffers_bytes', 'node_memory_Cached_bytes', 'node_memory_MemAvailable_bytes',
                'node_memory_MemFree_bytes', 'node_memory_MemTotal_bytes', 'node_memory_MemUsed_bytes',
                'node_memory_usage_percentage']
NODES_NETWORK = ['node_network_receive_bytes_total', 'node_network_receive_drop_total',
                 'node_network_receive_errs_total', 'node_network_receive_packets_total',
                 'node_network_transmit_bytes_total', 'node_network_transmit_drop_total',
                 'node_network_transmit_errs_total', 'node_network_transmit_packets_total']
DEPLOYMENTS = ['replicas', 'ready_replicas', 'available_replicas']
PODS = ['cpu_usage', 'memory_usage', 'restarts']
EDGE_STORAGE = ['minio_bucket_usage_object_total', 'minio_bucket_usage_total_bytes', 'minio_node_disk_free_bytes',
                'minio_node_disk_total_bytes', 'minio_node_disk_used_bytes', 'minio_s3_requests_total']
SERRANO_DEPLOYMENTS = ['cpu_usage', 'memory_usage', 'restarts', 'phase']


class PMDSList(TableList):

    def __init__(self, tables):
        self.__tables = tables

    def to_json_object(self, columns: List['str'] = None, **kwargs):

        def filter_values(record):
            if columns is not None:
                return {k: v for (k, v) in record.values.items() if k in columns}
            return record.values

        return [filter_values(record) for table in self.__tables for record in table.records]

    def to_json_object_grouped_by(self, columns: List['str'] = None, fields: List['str'] = None, **kwargs):

        data = {}

        group_by_column = kwargs.get("group_by", None)
        labels = kwargs.get("labels", [])

        if not group_by_column or not fields:
            return self.to_json_object(columns, kwargs)

        fields_count = len(fields)

        for t_i in range(int(len(self.__tables) / fields_count)):

            s_i = t_i * fields_count
            e_i = ((t_i + 1) * fields_count)

            grouped_tables = self.__tables[s_i:e_i]

            records_count = len(grouped_tables[0].records)
            data[grouped_tables[0].records[0][group_by_column]] = []

            for r_i in range(records_count):
                entry = {}
                for f_i in range(fields_count):
                    k = grouped_tables[f_i].records[r_i].values
                    entry[k["_field"]] = k["_value"]
                    entry["time"] = k["_time"]
                    for field_label in labels:
                        entry[field_label] = grouped_tables[f_i].records[r_i][field_label]
                data[grouped_tables[0].records[0][group_by_column]].append(entry)

        return data

    def cpus_to_json_object_grouped_by(self, cpus):

        data = {}
        s_i = 0

        for t_i in range(len(cpus)):
            e_i = s_i + (cpus[t_i] * 2)
            grouped_tables = self.__tables[s_i:e_i]
            s_i = e_i + 1
            records_count = len(grouped_tables[0].records)
            node_name = grouped_tables[0].records[0]["node_name"]
            data[node_name] = []

            for r_i in range(records_count):
                entry = {}
                for f_i in range(len(grouped_tables)):
                    k = grouped_tables[f_i].records[r_i].values
                    entry[k["_field"]] = k["_value"]
                    entry["time"] = k["_time"]
                data[node_name].append(entry)

        return data


class DataEngine:

    def __init__(self, config):

        client = InfluxDBClient(url="https://%s:%s" % (config["address"], config["port"]),
                                token=config["token"],
                                org=config["org"],
                                timeout=300000)

        self.__query_api = client.query_api()

    def query_nodes(self, bucket, start, **kwargs):

        data = {}
        cpus = []
        cpus_by_node = {}

        stop = kwargs.get("stop", None)
        node_name = kwargs.get("node_name", None)
        field_measurement = kwargs.get("field_measurement", None)
        format = kwargs.get("format", 'compact')
        group = kwargs.get("group", None)

        filter_query = ['r._measurement == "nodes"']
        range_query = ['start: %s' % start]

        if format == "compact" and not group:
            group = "general"

        if group == "general":
            target_fields = NODES_GENERAL
            filter_query.append('r.group == "%s"' % group)
        elif group == "storage":
            target_fields = NODES_STORAGE
            filter_query.append('r.group == "%s"' % group)
        elif group == "memory":
            target_fields = NODES_MEMORY
            filter_query.append('r.group == "%s"' % group)
        elif group == "network":
            target_fields = NODES_NETWORK
            filter_query.append('r.group == "%s"' % group)
        elif group == "cpu":
            filter_query.append('r.group == "%s"' % group)
            target_fields = None

            query = 'from(bucket:"%s") |> ' \
                    'range(start: -30m) |> ' \
                    'filter(fn:(r) => r._measurement == "nodes" and r.group == "cpu" and r._field=="cpu_0_idle") ' \
                    '|> last() |> distinct(column: "tag")' % bucket

            for t in self.__query_api.query(query):
                for r in t.records:
                    cpus_by_node[r["node_name"]] = int(r["node_cpus"])
                    cpus.append(int(r["node_cpus"]))

        if node_name:
            filter_query.append('r.node_name == "%s"' % node_name)
            cpus = [cpus_by_node[node_name] if node_name in cpus_by_node else 0]
        if field_measurement:
            filter_query.append('r._field == "%s"' % field_measurement)
            target_fields = [field_measurement]
        if stop:
            range_query.append('stop: %s' % stop)

        filters = " and ".join(filter_query)
        flux_query = 'from(bucket: "%s") |> range(%s) |> filter(fn: (r) => %s)' % (bucket,
                                                                                   ','.join(range_query),
                                                                                   filters)

        try:

            if format == "compact":
                tables = PMDSList(self.__query_api.query(flux_query))

                if target_fields is not None:
                    data = tables.to_json_object_grouped_by(fields=target_fields, group_by="node_name")
                else:
                    data = tables.cpus_to_json_object_grouped_by(cpus)
            else:
                data = PMDSList(self.__query_api.query(flux_query)).to_json_object(columns=["group", "_field",
                                                                                            "_time", "_value",
                                                                                            "node_name"])

        except Exception as err:
            logger.error("Unable to query InfluxDB service")
            logger.error(str(err))

        return data

    def query_persistent_volumes(self, bucket, start, **kwargs):

        data = {}

        stop = kwargs.get("stop", None)
        volume_name = kwargs.get("name", None)
        format = kwargs.get("format", 'compact')

        filter_query = ['r._measurement == "persistentVolumes"']
        range_query = ['start: %s' % start]

        if volume_name:
            filter_query.append('r.name== "%s"' % volume_name)
        if stop:
            range_query.append('stop: %s' % stop)

        filters = " and ".join(filter_query)
        flux_query = 'from(bucket: "%s") |> range(%s) |> filter(fn: (r) => %s)' % (bucket,
                                                                                   ','.join(range_query),
                                                                                   filters)

        try:

            if format == "compact":
                data = PMDSList(self.__query_api.query(flux_query)).to_json_object_grouped_by(
                    fields=["capacity_storage"],
                    group_by="name")

            else:
                data = PMDSList(self.__query_api.query(flux_query)).to_json_object(columns=["name", "_field",
                                                                                            "_time", "_value"])
        except Exception as err:
            logger.error("Unable to query InfluxDB service")
            logger.error(str(err))

        return data

    def query_deployments(self, bucket, start, **kwargs):

        data = {}

        name = kwargs.get("name", None)
        namespace = kwargs.get("namespace", None)
        stop = kwargs.get("stop", None)
        format = kwargs.get("format", 'compact')

        filter_query = ['r._measurement == "deployments"']
        range_query = ['start: %s' % start]

        if name:
            filter_query.append('r.name == "%s"' % name)
        if namespace:
            filter_query.append('r.namespace == "%s"' % namespace)
        if stop:
            range_query.append('stop: %s' % stop)

        filters = " and ".join(filter_query)
        flux_query = 'from(bucket:"%s") |> range(%s) |> filter(fn: (r) => %s )' % (bucket,
                                                                                   ','.join(range_query),
                                                                                   filters)
        try:
            if format == "compact":
                data = PMDSList(self.__query_api.query(flux_query)).to_json_object_grouped_by(fields=DEPLOYMENTS,
                                                                                              group_by="name")
            else:
                data = PMDSList(self.__query_api.query(flux_query)).to_json_object(columns=["name", "namespace",
                                                                                            "_field", "_time",
                                                                                            "_value"])
        except Exception as err:
            logger.error("Unable to query InfluxDB service")
            logger.error(str(err))

        return data

    def query_pods(self, bucket, start, **kwargs):

        data = {}

        stop = kwargs.get("stop", None)
        name = kwargs.get("name", None)
        node = kwargs.get("node_name", None)
        phase = kwargs.get("phase", None)
        namespace = kwargs.get("namespace", None)
        format = kwargs.get("format", 'compact')

        filter_query = ['r._measurement == "pods"']
        range_query = ['start: %s' % start]

        if namespace:
            filter_query.append('r.namespace == "%s"' % namespace)
        if name:
            filter_query.append('r.name == "%s"' % name)
        if node:
            filter_query.append('r.node == "%s"' % node)
        if phase:
            filter_query.append('r.phase == "%s"' % phase)
        if stop:
            range_query.append('stop: %s' % stop)

        filters = " and ".join(filter_query)
        flux_query = 'from(bucket:"%s") |> range(%s) |> filter(fn: (r) => %s )' % (bucket,
                                                                                   ','.join(range_query),
                                                                                   filters)

        try:

            if format == "compact":
                data = PMDSList(self.__query_api.query(flux_query)).to_json_object_grouped_by(fields=PODS,
                                                                                              group_by="name")
            else:
                data = PMDSList(self.__query_api.query(flux_query)).to_json_object(columns=["name", "namespace", "node",
                                                                                            "phase", "_field", "_time",
                                                                                            "_value"])
        except Exception as err:
            logger.error("Unable to query InfluxDB service")
            logger.error(str(err))

        return data

    def query_edge_storage_devices(self, bucket, start, **kwargs):

        data = {}

        stop = kwargs.get("stop", None)
        name = kwargs.get("name", None)
        node = kwargs.get("node_name", None)
        format = kwargs.get("format", 'compact')

        filter_query = ['r._measurement == "edge_storage"']
        range_query = ['start: %s' % start]

        if name:
            filter_query.append('r.name == "%s"' % name)
        if node:
            filter_query.append('r.node == "%s"' % node)
        if stop:
            range_query.append('stop: %s' % stop)

        filters = " and ".join(filter_query)
        flux_query = 'from(bucket:"%s") |> range(%s) |> filter(fn: (r) => %s )' % (bucket,
                                                                                   ','.join(range_query),
                                                                                   filters)
        try:

            if format == "compact":
                data = PMDSList(self.__query_api.query(flux_query)).to_json_object_grouped_by(fields=EDGE_STORAGE,
                                                                                              group_by="name")
            else:
                data = PMDSList(self.__query_api.query(flux_query)).to_json_object(columns=["name", "node", "_field",
                                                                                            "_time", "_value"])
        except Exception as err:
            logger.error("Unable to query InfluxDB service")
            logger.error(str(err))

        return data

    def query_serrano_deployments(self, deployment_uuid, bucket, start, **kwargs):

        data = {}

        stop = kwargs.get("stop", None)
        cluster_uuid = kwargs.get("cluster_uuid", None)
        node = kwargs.get("node_name", None)
        format = kwargs.get("format", 'compact')

        filter_query = ['r._measurement == "serrano_deployments"', 'r.deployment_uuid == "%s"' % deployment_uuid]
        range_query = ['start: %s' % start]

        if node:
            filter_query.append('r.node == "%s"' % node)
        if cluster_uuid:
            filter_query.append('r.cluster_uuid == "%s"' % cluster_uuid)
        if stop:
            range_query.append('stop: %s' % stop)

        filters = " and ".join(filter_query)
        flux_query = 'from(bucket:"%s") |> range(%s) |> filter(fn: (r) => %s )' % (bucket,
                                                                                   ','.join(range_query),
                                                                                   filters)
        try:
            if format == "compact":
                data = PMDSList(self.__query_api.query(flux_query)).to_json_object_grouped_by(fields=SERRANO_DEPLOYMENTS,
                                                                                              group_by="name",
                                                                                              labels=["group_id"])
            else:
                data = PMDSList(self.__query_api.query(flux_query)).to_json_object(columns=["deployment_uuid",
                                                                                            "cluster_uuid",
                                                                                            "name",
                                                                                            "group_id",
                                                                                            "node",
                                                                                            "_field",
                                                                                            "_time",
                                                                                            "_value"])
        except Exception as err:
            logger.error("Unable to query InfluxDB service")
            logger.error(str(err))

        return data
