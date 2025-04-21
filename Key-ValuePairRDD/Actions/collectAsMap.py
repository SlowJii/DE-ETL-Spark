from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("SlowJii").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

data = sc.parallelize([("Dat-debt", 5.0), ("Hoang-debt", 7.2),
                       ("Dat-debt", 4.0), ("Hoang-debt", 6.3),
                       ("Quan-debt", 18.0)
                       ])

print(data.collectAsMap())