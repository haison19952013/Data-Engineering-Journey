import configparser
import os.path

from pyspark import SparkConf


def get_spark_conf():
    util_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"util_dir: {util_dir}")

    conf = configparser.ConfigParser()
    conf.read(util_dir + "/../spark.conf")

    spark_conf = SparkConf()
    for (k, v) in conf.items("SPARK"):
        spark_conf.set(k, v)

    return spark_conf
