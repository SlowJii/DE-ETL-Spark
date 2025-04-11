"""
PRACTICE
Ap dung tung logic cu theo tung phan vung
Chia du lieu thanh 2 phan vung:
    - phan vung 1 tang them 1 don vi moi phan tu
    - phan vung 2 gap doi gia tri phan tu
"""

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("SlowJii").setMaster("local[*]").set("spark.executor.memory", "4g")
sc= SparkContext(conf=conf)

numbersRdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10],2)
# partitions_0 = 1 2 3 4 5
# partitions_1 = 6 7 8 9 10
# (index, item)
# index = 0 : item + 1
# index = 1 : item * 2
numbersRddWithIndex = numbersRdd.mapPartitionsWithIndex(
    lambda index, iter: [(index, item) for item in iter]
)

result = numbersRdd.mapPartitionsWithIndex(
    lambda index, iter: ([(index, item + 1) if index == 0 else (index, item * 2) for item in iter])
)

print(f"Phan vung ban dau:            {numbersRdd.glom().collect()}")
print(f"Phan vung sau khi danh index: {numbersRddWithIndex.glom().collect()}")
print(f"Ket qua:                      {result.glom().collect()}")
