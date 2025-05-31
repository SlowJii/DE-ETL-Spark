import random

from pyspark.sql import SparkSession


# Co the khoi tao RDD dua tren SparkSession
spark = SparkSession.builder \
    .appName("SlowJii") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

rdd = spark.sparkContext.parallelize(range(1,11)) \
    .map(lambda x: (x, random.randint(0,99)*x ))

"""
key     value
1       random
2       random
3       random
4       random
5       random  
5       random
6       random  
"""
print(rdd.collect())

# Schema duoc hieu la cau truc cua du lieu (dinh dang du lieu)
schema = ["key", "value"]
df = spark.createDataFrame(rdd, schema).show() #show() giong nhu print nma in ra theo dang DataFrame

