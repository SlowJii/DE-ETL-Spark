from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("SlowJii").setMaster("local[*]").set("spark.executor.memory", "4g")
sc= SparkContext(conf=conf)

numbersRdd = sc.parallelize([1, 2, 3, 4, 5,6,7,8,9,10], 3)

print(f"RDD ban dau: {numbersRdd.glom().collect()}")

"""
Ham "reduce" trong PySpark hoac RDD noi chung ap dung mot ham nhi phan (binary function), tuc la ham chi
nhan 2 doi so len cac phan tu RDD de thu gon (reduce) chung thanh mot gia tri duy nhat
"""

def sum(a: int, b: int) -> int:
    print(f"a = {a}, b = {b} => ({a + b}) \n")
    return a + b

print(numbersRdd.reduce(sum))