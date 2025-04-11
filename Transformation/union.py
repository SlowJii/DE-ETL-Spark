from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("SlowJii").setMaster("local[*]").set("spark.executor.memory", "4g")
sc= SparkContext(conf=conf)

"""
Do tinh bat bien cua RDD nen ta phai dung union de ket noi 2 RDD lai voi nhau
Union co the noi cac kieu du lieu khac nhau lai voi nhau
"""

rdd1 = sc.parallelize([
    {"id": 1, "name": "Hoang"},
    {"id": 2, "name": "Dat"},
    {"id": 3, "name": "Quan"}

])
rdd2 = sc.parallelize([6,7,8,9,10])

rdd3 = rdd1.union(rdd2)
print(rdd3.collect())