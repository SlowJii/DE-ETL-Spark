"""
Ban chat cua Reduce By Key giong nhu Reduce nma khac o cho la reduce tren key, con reduce la thuc hien tren RDD
"""

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("SlowJii").setMaster("local[*]").set("spark.executor.memory", "4g")
sc= SparkContext(conf=conf)

rdd = sc.parallelize([
    "Con cao va chum nho, chum nho do o that cao, con cao khong an duoc nho nen che la nho co ngon la bao"
])
rdd2 = rdd.flatMap(lambda x: x.split(" "))

pairRDD = rdd2.map(lambda x: (len(x), x))
