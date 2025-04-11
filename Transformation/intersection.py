from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("SlowJii").setMaster("local[*]").set("spark.executor.memory", "4g")
sc= SparkContext(conf=conf)

rdd1 = sc.parallelize([1,2,3,4,5])
rdd2 = sc.parallelize([1,123,3,5,11,9])

rdd3 = rdd1.intersection(rdd2).collect()
print(rdd3)

"""
Ham intersection tra ve cac phan tu giong nhau giua 2 rdd
"""