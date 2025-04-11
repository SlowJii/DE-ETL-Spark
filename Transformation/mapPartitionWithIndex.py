# index & func
# index co the hieu la id cua so phan vung
#    [12,124,4536]
#index 0, 1,  2

# func la mot 1 logic, lap qua cac phan tu trong index

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("SlowJii").setMaster("local[*]").set("spark.executor.memory", "4g")
sc = SparkContext(conf = conf)

numbersRdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10],2) # bam RDD thanh 2 phan
# partition_0 = 1 2 3 4 5
# partition_1 = 6,7,8,9,10

# (1,0), (2,0), (3,0), (4,0), (5,0)
# (6,1), (7,1), (8,1), (9,1), (10,1)
# logic = (item,index)
# doi voi moi phan tu trong phan vung 0,1 tuong ung thi no se tao ra (phan tu, index) ghep noi lai voi nhau
result = numbersRdd.mapPartitionsWithIndex(
    lambda index, iter: [(item, index) for item in iter]
).collect()

print(result)




