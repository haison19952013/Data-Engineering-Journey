import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col

import util.config as conf
from util.logger import Log4j

if __name__ == '__main__':
    working_dir = os.getcwd()
    print("working_dir: " + working_dir)

    spark_conf = conf.get_spark_conf()

    spark = SparkSession \
        .builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    surveyDf = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(path="/data/spark-sql/survey.csv")

    log.info("survey df schema: ")
    surveyDf.printSchema()

    surveyDf.createOrReplaceTempView("survey_view")
    countDf = spark.sql(
        """
        select Country, count(1) as count 
        from survey_view 
        where Age < 40 
        group by Country 
        order by count(1) desc
        """)

    log.info("countDf: ")
    countDf.show()

    genderDf = surveyDf \
        .select(col('Gender'),
                col('Country'),
                f.when(('male' == f.lower(col('Gender'))) | ('m' == f.lower(col('Gender'))), 1)
                .otherwise(0).alias('num_male'),
                f.when(('female' == f.lower(col('Gender'))) | ('f' == f.lower(col('Gender'))), 1)
                .otherwise(0).alias('num_female'))

    genderDf.show()

    aggDf = genderDf \
        .groupBy('Country') \
        .agg(f.sum('num_male').alias('num_male'),
             f.sum('num_female').alias('num_female')) \
        .orderBy('Country')

    aggDf.show()

    spark.stop()
