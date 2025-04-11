from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("SlowJii").setMaster("local[*]").set("spark.executor.memory", "4g")
sc= SparkContext(conf=conf)

numbers = sc.parallelize([1,2,3,4,5,6,7,8,9,10],2)
# partitions_0 = 1 2 3 4 5
# partitions_1 = 6 7 8 9 10
"""
Take se di tu phan vung dau tien den phan tu duoc truyen vao ham take
Neu phan tu duoc truyen vao ham take vuot ngoai phan tu co trong rdd, Take se in ra toan bo gia tri co ben trong rdd
"""
print(numbers.take(6))  # Ket qua: [1,2,3,4,5,6]
print(numbers.take(11)) # Ket qua: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]