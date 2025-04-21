from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("SlowJii").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

data = sc.parallelize([("Dat-debt", 5.0), ("Hoang-debt", 7.2),
                       ("Dat-debt", 4.0), ("Hoang-debt", 6.3),
                       ("Quan-debt", 18.0)
                       ]).countByKey()
"""
Count by Key se tra ve mot dict python bao gom key va value
Trong do key la key xuat hien trong data, value la so lan no xuat hien
defaultdict(<class 'int'>, {'Dat-debt': 2, 'Hoang-debt': 2, 'Quan-debt': 1})
"""
print(data) # defaultdict(<class 'int'>, {'Dat-debt': 2, 'Hoang-debt': 2, 'Quan-debt': 1})
# Ep ket qua vao dict in ra cho de nhin
print(dict(data)) # {'Dat-debt': 2, 'Hoang-debt': 2, 'Quan-debt': 1}
