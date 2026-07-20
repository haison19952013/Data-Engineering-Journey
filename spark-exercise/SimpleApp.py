"""SimpleApp.py"""
from pathlib import Path

from pyspark.sql import SparkSession

logFile = str(Path(__file__).resolve().parents[1] / "kafka-streaming-pipeline" / "README.md")
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()