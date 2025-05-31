from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime

spark = SparkSession.builder \
    .appName("SlowJii") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

textFile = spark.read.text("/home/lehoang/PycharmProjects/DE-ETL-Spark/Data/dataDE.txt")
textFile.show(truncate=False)