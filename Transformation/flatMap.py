from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local[*]").setAppName("SlowJii").set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

fileRdd = sc.textFile("../Data/dataDE.txt")
flatMapRdd = fileRdd.flatMap(lambda line: line.split(" "))

#for line in flatMapRdd.collect():
#    print(line)
print(flatMapRdd.collect())


