from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder \
    .appName("SlowJii") \
    .master("local[*]") \
    .config("spark.executor.memory", "6g") \
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

from pyspark.sql.functions import col, struct, lit
from pyspark.sql.functions import upper
from pyspark.sql.functions import length

dataFile = spark.read.schema(jsonSchema).json("/home/lehoang/Documents/data/2015-03-01-17.json")

#------------------SORT------------------
#dataFile.sort(col("id")).show()

#------------------ODER BY---------------
# oderBy sap xep giam dan = them .desc()
#dataFile.select(col("id")).orderBy(col("id").desc()).show()

# Sort cot id theo tang dan, Sort cot actor.id giam dan
#dataFile.orderBy([col("id"), col("actor.id")], ascending = [True, False]).select(col("id"), col("actor.id").alias("actor_id")).show()
"""
Khong kha quan boi vi oderBy sort id truoc, ma moi id se di kem voi mot actor.id tuong ung
Sort theo ban ghi, khong the co truong hop 1 ban ghi sort 2 kieu
====> Tao cot moi bang with column, su dung ham UDF
"""

dataFileStruct = dataFile.withColumn(
    "actor",
    struct(
        col("actor.id").alias("id"),
        col("actor.login").alias("login"),
        col("actor.gravatar_id").alias("gravatar_id"),
        col("actor.url").alias("url"),
        col("actor.avatar_url").alias("avatar_url"),
        lit("Hehe").alias("id2")
        # cot du lieu long cot du lieu
        # ban ghi JSON long trong nhieu ban ghi JSON
    )
)
from pyspark.sql.functions import udf
def transformData(id):
    count = 1
    return count + int(id)

myUDF = udf(transformData, StringType())
df = dataFileStruct.withColumn("actor.id2", myUDF(col("id")))

df.orderBy([col("actor.id"), col("actor.id2")], ascending=[True, False])
df.select(col("actor.id"), col("actor.id2")).show()