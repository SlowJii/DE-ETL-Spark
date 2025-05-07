from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import LongType, StringType, StructType, StructField, IntegerType, FloatType, MapType, ArrayType, \
    DateType, TimestampType, DecimalType, BooleanType
from datetime import datetime



spark = SparkSession.builder \
    .appName("SlowJii") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") .getOrCreate()

#khoi tao doi tuong du lieu mau

dataObject = [
    {"id": 1, "name": "Hoang"},
    {"id": 2, "name": "Dat"},
    {"id": 3, "name": "Quan"}
]


# Khoi tao 1 ban ghi bang class Row cua pyspark.sql

data = spark.sparkContext.parallelize([
    Row(1,"Hoang",23),
    Row(2,"Dat",25),
    Row(3,"Quan",23),
    Row(None,None,None),
    Row(4,"",16)
])


# StrucType la cau truc du lieu tong the
# StructField la di vao tung row, tung truong du lieu se co cau truc nhu the nao


schema = StructType([
    StructField("id", LongType(), True), # cot id voi kieu du lieu Long, dc phep null
    StructField("name", StringType(), True),
    StructField("age", LongType(), True)
])
df = spark.createDataFrame(data,schema).show()

# nullable tuc la truong du lieu do duoc phep NULL hay khong, neu nullable = False thi truong du lieu kh dc phep NULL


