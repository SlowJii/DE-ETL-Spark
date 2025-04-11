from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("SlowJii").setMaster("local[*]").set("spark.executor.memory", "4g")
sc= SparkContext(conf=conf)

rdd = sc.parallelize([
    "Con cao va chum nho, chum nho do o that cao, con cao khong an duoc nho nen che la nho co ngon la bao"
])

rdd2 = rdd.flatMap(lambda x: x.split(" "))
print(rdd2.collect())
# logic = (item, length)
# (con,3), (cao, 3), (va,2)
"""
Su khac nhau giua map va flatmap trong Spark RDD
    - map: Ap dung mot ham len tung phan tu ben trong RDD kieu 1-1 va tra ve 1 RDD moi co cung so luong ptu
    - flatmap: Ap dung mot ham len tung phan tu ben trong RDD nhung cho phep ham tra ve tap hop cac phan tu (dict, tuples)
    Ket qua cua flatmap co the co so luong phan tu khac so voi RDD dau vao 
"""

pairRDD = rdd2.map(lambda x: (x, len(x)))
print(pairRDD.collect())
#for pair in pairRDD.collect():
#    print(pair)
