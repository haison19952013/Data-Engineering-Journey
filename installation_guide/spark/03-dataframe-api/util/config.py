import configparser
import os

from pyspark import SparkConf


def get_spark_conf():
    conf = configparser.ConfigParser()

    util_folder = os.path.dirname(os.path.abspath(__file__))
    print("util folder: " + util_folder)

    conf.read(util_folder + "/../spark.conf")

    spark_conf = SparkConf()

    for (k, v) in conf.items("SPARK"):
        spark_conf.set(k, v)
    return spark_conf
