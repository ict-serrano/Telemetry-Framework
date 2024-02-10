
class PMDSConfiguration:

    def __init__(self, config):
        self.__config = config

    def get_log_level(self):
        return "INFO" if "log_level" not in self.__config else self.__config["log_level"]

    def get_pmds_db(self):
        data = {"address": "", "port": "", "org": "", "token": ""}

        if "address" in self.__config["influxDB"]:
            data["address"] = self.__config["influxDB"]["address"]
        if "port" in self.__config["influxDB"]:
            data["port"] = self.__config["influxDB"]["port"]
        if "org" in self.__config["influxDB"]:
            data["org"] = self.__config["influxDB"]["org"]
        if "token" in self.__config["influxDB"]:
            data["token"] = self.__config["influxDB"]["token"]

        return data
    
    def get_rest_interface(self):

        data = {"address": "", "port": ""}

        if "address" in self.__config["rest_interface"]:
            data["address"] = self.__config["rest_interface"]["address"]
        if "port" in self.__config["rest_interface"]:
            data["port"] = self.__config["rest_interface"]["port"]
        return data
