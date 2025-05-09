from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder \
    .appName("SlowJii") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

csvFile = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/lehoang/Documents/data/flights_2008_7M.csv")
# Dua header len thanh cac cot c0,c1,c2...vv cho giong voi file csv
csvFile.show()