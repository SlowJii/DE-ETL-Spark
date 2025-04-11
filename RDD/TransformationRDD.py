from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("SlowJii").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf = conf)

number = [1,2,3,4,5,6,7,8,9,10]
rdd = sc.parallelize(number)

#su dung transformation de khoi tao rdd moi nham thay doi gia tri data (chung minh tinh bat bien cua rdd)
squareRdd = rdd.map(lambda i: i*i)
filterRdd = rdd.filter(lambda i: i > 4)

# [1,1x2,2,2x2,3,3x2....]
flatMapRdd = rdd.flatMap(lambda i: [i, i*2])

