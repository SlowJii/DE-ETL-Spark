import time

from pyspark import SparkContext, SparkConf
from random import Random

conf = SparkConf().setAppName("SlowJii").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

data = ["Hehe", "Haha", "Hihi", "Huhu", "Hoho"]

# them id random vao cac phan tu trong data
rdd = sc.parallelize(data)

#def numsPartition(iter):
    # khoi tao 1 so ngau nhien cho phan vung data
#    rand = Random(int(time.time()*1000) + Random().randint(0,1000))
#    return [f"{item}:{rand.randint(0,100)}" for item in iter]

#result = rdd.mapPartitions(numsPartition)
#print(result.collect())


result = rdd.mapPartitions(
    lambda item: map(
        lambda l: f"{l}:{(Random(int(time.time()*1000) + Random().randint(0,1000))).randint(0,100)}",
        item
        # Ham map nhan 2 doi so la "l" va "item"
        # map se ap dung ham lambda l: len tung phan tu "item"
    )
)
print(result.collect())