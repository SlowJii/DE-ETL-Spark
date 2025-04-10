#import library
from pyspark import SparkContext

#khoi tao sparkContext
sc = SparkContext("local", "SlowJii")

#khoi tao doi tuong du lieu
data = [
    {"id": 1, "name": "Hoang"},
    {"id": 2, "name": "Dat"},
    {"id": 3, "name": "Quan"}
]

#moc data vao rdd
rdd = sc.parallelize(data)

print(rdd.collect()) # in ra duoi dang List
print(f"number of data: {rdd.count()}")
print(f"first value of data: {rdd.first()}")
