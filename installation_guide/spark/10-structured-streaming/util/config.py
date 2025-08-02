import configparser
import os.path

from pyspark import SparkConf


class NetcatConf:
    host: str
    port: int

    def __str__(self):
        return f"NetcatConf(host: {self.host}, port: {self.port})"

    def from_dict(self, d: dict):
        self.host = d["nc.host"]
        self.port = int(d["nc.port"])


class Config:
    def __init__(self):
        util_dir = os.path.dirname(os.path.abspath(__file__))
        print(f"util_dir: {util_dir}")

        conf = configparser.ConfigParser()
        conf.read(util_dir + "/../spark.conf")

        self.conf = conf
        self.spark_conf = self._get_spark_conf()
        self.nc_conf = self._get_nc_conf()

    def _get_spark_conf(self):
        spark_conf = SparkConf()

        for (k, v) in self.conf.items("SPARK"):
            spark_conf.set(k, v)

        return spark_conf

    def _get_nc_conf(self):
        properties = {}

        for (k, v) in self.conf.items("NC"):
            properties[k] = v

        nc_conf = NetcatConf()
        nc_conf.from_dict(properties)
        return nc_conf
