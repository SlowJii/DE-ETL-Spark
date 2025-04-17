"""
Ban chat cua Reduce By Key giong nhu Reduce nma khac o cho la reduce tren key, con reduce la thuc hien tren RDD
"""
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("SlowJii").setMaster("local[*]").set("spark.executor.memory", "4g")
sc= SparkContext(conf=conf)

data = sc.parallelize([("Dat-debt", 5.0), ("Hoang-debt", 7.2),
                       ("Dat-debt", 4.0), ("Hoang-debt", 6.3),
                       ("Quan-debt", 9.0)
                       ])
# Tinh toan tong no cua Dat, Hoang, Quan
# Nhom tat ca cac ban ghi co Dat-deb, Hoang-debt va Quan-debt bang GroupByKey
# Sau do lay cac value cong lai voi nhau se ra tong no (total-debt)
# ReduceByKey se vua gop du lieu, gop cac key co gia tri giong nhau va tinh toan theo logic yeu cau tren cac cap value

bill = data.reduceByKey(lambda key, value: key + value).collect() # key = key thi value + value
print(bill)

"""
[A,1]
[A,2]
[B,1]
[B,5]
[C,3]
[C,8]
Truong hop nay key-value cung xu li nhu tren
key = key thi value + value: A = A => 1+2 => [A,3]
                             B = B => 1+5 => [B,6]
                             C = C => 3+8 => [C,11]
ReduceByKey chinh la GroupByKey: Vua giam tat cac cai ban ghi ve ban ghi co chung key, khac value nma ReduceByKey se giam them lan nua bang cach ap dung ham logic vao cac value, dan den viec cac ban ghi bi giam xuong hon so voi GroupByKey
"""

