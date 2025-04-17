from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("SlowJii").setMaster("local[*]").set("spark.executor.memory", "4g")
sc= SparkContext(conf=conf)

data = sc.parallelize([("Dat-debt", 5.0), ("Hoang-debt", 7.2),
                       ("Dat-debt", 4.0), ("Hoang-debt", 6.3),
                       ("Quan-debt", 9.0)
                       ])
"""
LookUp la tim kiem Value, ban ghi cua Key day (dua tren Key)
"""
print(data.lookup("Dat-debt"))
print(data.lookup("Hoang-debt"))
print(data.lookup("Quan-debt"))
print(data.lookup("Nam-debt")) # Tra ve mot List rong

