import uuid



class CentralHandlerConfiguration:

    def __init__(self, config):
        self.__config = config

    def get_active_monitoring(self):
        return self.__config["active_monitoring"]

    def set_active_monitoring(self, status):
        self.__config["active_monitoring"] = status

    def get_log_level(self):
        return "INFO" if "log_level" not in self.__config else self.__config["log_level"]

    def get_uuid(self):
        return str(uuid.uuid4()) if "uuid" not in self.__config else self.__config["uuid"]

    def get_query_interval(self):
        return 60 if "query_internal" not in self.__config else self.__config["query_internal"]

    def set_query_interval(self, interval):
        self.__config["query_internal"] = interval

    def get_query_timeout(self):
        return 5 if "query_timeout" not in self.__config else self.__config["query_timeout"]

    def set_query_timeout(self, timeout):
        self.__config["query_timeout"] = timeout

    def get_operational_db(self):
        data = {"address": "", "username": "", "password": "", "dbName": ""}

        if "address" in self.__config["operational_db"]:
            data["address"] = self.__config["operational_db"]["address"]
        if "username" in self.__config["operational_db"]:
            data["username"] = self.__config["operational_db"]["username"]
        if "password" in self.__config["operational_db"]:
            data["password"] = self.__config["operational_db"]["password"]
        if "dbName" in self.__config["operational_db"]:
            data["dbName"] = self.__config["operational_db"]["dbName"]

        return data

    def get_rest_interface(self):

        data = {"address": "", "port": ""}

        if "address" in self.__config["rest_interface"]:
            data["address"] = self.__config["rest_interface"]["address"]
        if "port" in self.__config["rest_interface"]:
            data["port"] = self.__config["rest_interface"]["port"]

        if "exposed_service" not in self.__config["rest_interface"]:
            data["exposed_service"] = "https://%s:%s" % (data["address"], data["port"])
        else:
            data["exposed_service"] = self.__config["rest_interface"]["exposed_service"]

        return data

    def get_cloud_storage_locations(self):

        data = {"address": ""}

        if "address" in self.__config["cloud_storage_locations"]:
            data["address"] = self.__config["cloud_storage_locations"]["address"]

        return data
