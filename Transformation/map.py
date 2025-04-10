from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName("SlowJii").setMaster("local[*]").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

fileRdd = sc.textFile("../Data/dataDE.txt")

allCapsRdd = fileRdd.map(lambda x: x.upper())
print(allCapsRdd.collect()) # print duoi dang List

#in ra tung dong mot
for line in allCapsRdd.collect():
    print(line)



