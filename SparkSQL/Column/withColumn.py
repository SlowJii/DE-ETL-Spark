from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder \
    .appName("SlowJii") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

jsonFile = spark.read.json("/home/lehoang/Documents/data/2015-03-01-17.json")

# Muon xu li voi du lieu lon (big data) thi phai xu li voi du lieu be
jsonSchema = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    #StructType giong nhu mot Object(Doi tuong) dinh nghia cac StrucField
    #============ACTOR====================
    StructField("actor", StructType([
        StructField("id", LongType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True)
    ]), True),
    #============REPO====================
    StructField("repo", StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True)
    ]), True),
    #============PAYLOAD==================
    StructField("payload", StructType([
        StructField("action", StringType(), True),
        #============ISSUE====================
        StructField("issue", StructType([
            StructField("url", StringType(), True),
            StructField("labels_url", StringType(), True),
            StructField("comments_url", StringType(), True),
            StructField("events_url", StringType(), True),
            StructField("html_url", StringType(), True),
            StructField("id", LongType(), True),
            StructField("number", LongType(), True),
            StructField("title", StringType(), True),
            #============USER====================
            StructField("user", StructType([
                StructField("login", StringType(), True),
                StructField("id", LongType(), True),
                StructField("avatar_url", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("followers_url", StringType(), True),
                StructField("following_url", StringType(), True),
                StructField("gists_url", StringType(), True),
                StructField("starred_url", StringType(), True),
                StructField("subscriptions_url", StringType(), True),
                StructField("organizations_url", StringType(), True),
                StructField("repos_url", StringType(), True),
                StructField("events_url", StringType(), True),
                StructField("received_events_url", StringType(), True),
                StructField("type", StringType(), True),
                StructField("site_admin", BooleanType(), True)
            ]), True),
            #============LABELS====================
            StructField("labels", ArrayType(StructType([
                StructField("url", StringType(), True),
                StructField("name", StringType(), True),
                StructField("color", StringType(), True),
            ])),True),
            #============RESTOFALL====================
            StructField("state", LongType(), True),
            StructField("locked", BooleanType(), True),
            StructField("assignee", StringType(), True),
            StructField("milestone", StringType(), True),
            StructField("comments", LongType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at",TimestampType(),True),
            StructField("closed_at", TimestampType(), True),
            StructField("body", StringType(), True)
        ]), True)
    ]), True),
    StructField("public", BooleanType(), True),
    StructField("created_at", TimestampType(), True)
])

from pyspark.sql.functions import col
from pyspark.sql.functions import upper
from pyspark.sql.functions import length
from pyspark.sql.functions import lit
from pyspark.sql.functions import struct

dataFile = spark.read.schema(jsonSchema).json("/home/lehoang/Documents/data/2015-03-01-17.json")

"""
withColumn
colName: Name of column
column: Value of column
Co 3 chuc nang moi can luu y
    - them 1 cot moi neu colName khong ton tai
    - ghi de len cot hien tai neu colName da ton tai
    -   col(), 
        lit(): gan gia tri cu the (hang so) vao mot cot moi hoac cot hien co
"""
# tao them cot moi id2 va gan gia tri cua no bang gia tri cua cot id chia du cho 2
#dataFile.withColumn("id2", lit(col("id") % 2)).select(col("id"), col("id2")).show()

# them cot id2 vao ben trong actor
#dataFile.withColumn("actor.id2", lit(col("id") % 2)).select(col("actor.id"), col("actor.id2")).show()
# mac du dung ve mat li thuyet nhung cau lenh tren khong thanh cong do dataFile dang duoc doc tu schema, va kh the pha duoc schema
# gio muon them mot cot vao data thi phai lam gi ?
# Solution: Su dung ham struct()

dataFileStruct = dataFile.withColumn(
    "actor",
    struct(
        col("actor.id").alias("id"),
        col("actor.login").alias("login"),
        col("actor.gravatar_id").alias("gravatar_id"),
        col("actor.url").alias("url"),
        col("actor.avatar_url").alias("avatar_url"),
        lit(col("id") % 2).alias("id2")
        # cot du lieu long cot du lieu
        # ban ghi JSON long trong nhieu ban ghi JSON
    )
)

dataFileStruct.select(col("actor.login"), col("actor.id2")).show()