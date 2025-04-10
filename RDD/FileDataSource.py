from pyspark import SparkContext, SparkConf

#Spark chay tren nen in-memory (CPU,RAM)
#local[*] tuc la lay toan bo luong cua CPU, co the set 4,8,10 tuy chon vao so luong CPU minh co
#spark.executor.memory, 4g tuc la set cho RDD 4GB RAM
conf = SparkConf().setAppName("SlowJii").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

fileRdd = sc.textFile("../Data/dataDE.txt")
print(fileRdd.collect())
print(f"number of data: {fileRdd.count()}")
print(f"first value of data: {fileRdd.first()}")