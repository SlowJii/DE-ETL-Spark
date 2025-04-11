from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("SlowJii").setMaster("local[*]").set("spark.executor.memory", "4g")
sc= SparkContext(conf=conf)

text = sc.parallelize(["O kia ai nhu co Tham, con bac Nam di xa moi ve ! Dit me con be xinh vcl"]) \
    .flatMap(lambda l : l.split(" ")) \
    .map(lambda lower: lower.lower())
#print(text.collect())

removeText = sc.parallelize(["dit me vcl"]) \
    .flatMap(lambda x : x.split(" "))
print(removeText.collect())
"""
Su dung ham subtract de bien doi du lieu theo y minh muon
    - chuyen ve lower,upper
    - loai bo tu cam
"""

niceText = text.subtract(removeText).collect()
print(niceText)


