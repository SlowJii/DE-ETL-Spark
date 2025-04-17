from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("SlowJii").setMaster("local[*]").set("spark.executor.memory", "4g")
sc= SparkContext(conf=conf)

"""
Join trong Spark cung tuong tu Join trong SQL
Thuong su dung trong bai toan dong bo du lieu
"""
data1 = sc.parallelize([(110,50,12), (127, 163.9), (125, 211.0),
                        (105,6.3), (163, 36.6), (110, 49.88)
                        ])
data_2 = sc.parallelize([(110, "Hoang"), (127, "Dat"), (125, "Quan"),
                        (105, "Phuc"), (163, "Khanh Sky"),(110, "Kha Banh")
                         ])
dataNew = data1.join(data_2)
print(f"Gia tri ban dau: {dataNew.collect()}")
print("--------------------------------------")
dataSort = dataNew.sortByKey(ascending=True)
print(f"Gia tri sau khi sap xep in theo hang doc")
for value in dataSort.collect():
    print(value)
print("--------------------------------------")
print(f"Tim kiem Key 110: {dataSort.lookup(110)}")
# -----------------------------------------------
print(f"Tim kiem Key 110: {dataNew.lookup(110)}")
"""
cau lenh dataNew.lookup gap loi PYTHONHASHSEED lien quan den yeu cau cua PySpark ve viec bam (hash) phai co 
tinh xac dinh khi thuc hien cac thao tac nhu LookUp. Van de xay ra vi ham bam cua Python cho string co the dc
ngaui nhien hoa theo mac dinh (kiem soat boi bien moi truong PYTHONHASHSEED) va cac yeu cau cua PySpark nhu LookUP
yeu cau hash mot cach nhat quan
"""
