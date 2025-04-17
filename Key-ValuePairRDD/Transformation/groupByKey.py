from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("SlowJii").setMaster("local[*]").set("spark.executor.memory", "4g")
sc= SparkContext(conf=conf)

"""
Group By Key co nghia la nhom cac dang du lieu co cung key lai voi nhau
"""
rdd = sc.parallelize([
    "Con cao va chum nho, chum nho do o that cao, con cao khong an duoc nho nen che la nho co ngon la bao"
])
rdd2 = rdd.flatMap(lambda x: x.split(" "))

pairRDD = rdd2.map(lambda x: (len(x), x))
groupByKeyRDD = pairRDD.groupByKey()

for key,value in groupByKeyRDD.collect():
    print(key, list(value)) # nhom bang list hay tuple deu in ra duoc

"""
Nhuoc diem cua Group By Key la kho thuc hien cac tinh toan
Ban chat cua no la chi nhom dc cac key, con tinh toan tren cac key hay value thi kha kho
==> Spark tao ra cai Reduce By Key de sua chua cai han che cua Group By Key
"""