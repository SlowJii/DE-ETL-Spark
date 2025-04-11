from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("SlowJii").setMaster("local[*]").set("spark.executor.memory", "4g")
sc= SparkContext(conf=conf)

valueRdd = sc.parallelize(
    ["one", 1, "two", 2, "three", 3, "two",1,2]
)
# Ham distinct() su dung de loai bo trung lap ben trong RDD
print(valueRdd.distinct().collect())