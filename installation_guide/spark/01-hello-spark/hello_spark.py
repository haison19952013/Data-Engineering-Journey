from pyspark.sql import *

if __name__ == "__main__":
    # Create a Spark session with the application name "HelloSpark"
    # and connect to the Spark master at spark://spark:7070
    spark = SparkSession.builder \
        .appName("HelloSpark") \
        .master("spark://spark:7077") \
        .getOrCreate()

    print("Start HelloSpark")

    data_list = [("Phuong", 31),
                 ("Huy", 31),
                 ("Bee", 25)]
    # Create a DataFrame from the list of tuples
    # and name the columns "Name" and "Age"
    # The DataFrame will be displayed in a tabular format
    # when df.show() is called
    df = spark.createDataFrame(data_list).toDF("Name", "Age")
    df.show()

    print("Stop HelloSpark")
    spark.stop()
