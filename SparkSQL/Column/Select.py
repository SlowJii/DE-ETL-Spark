from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder \
    .appName("SlowJii") \
    .master("local[*]") \
    .config("spark.executor.memory", "8g") \
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

jsonData = spark.read.schema(jsonSchema).json("/home/lehoang/Documents/data/2015-03-01-17.json")

from pyspark.sql.functions import col
from pyspark.sql.functions import upper
#jsonData.select(col("id"), col("id") > 2615567702).show()

# SELECT with alisas = SQL (as)
#jsonData.select(col("id").alias("user_id"), col("type").alias("user_type"), col("actor.id").alias("actor_id")).show()

# SELECT with expression - SELECT voi dieu kien
jsonData.select(
    col("id") * 2,
    upper(col("type").alias("Upper_Type"))
).show()

# SELECT ALL COLUMS
# jsonData.select("*").show()
#jsonData.select(jsonData.columns).show()

jsonData.select(
    col("id"),
    (col("id") < "2615567688").alias("id_smaller_2615567688")
).show()

#tinh toan tren Columns
jsonData.select(
    col("id"),
    (col("actor.id") - (col("actor.id")%2)).alias("actor_id_new")
).show()