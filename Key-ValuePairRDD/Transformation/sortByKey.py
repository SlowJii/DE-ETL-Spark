from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("SlowJii").setMaster("local[*]").set("spark.executor.memory", "4g")
sc= SparkContext(conf=conf)

data = sc.parallelize([("Dat-debt", 5.0), ("Hoang-debt", 7.2),
                       ("Dat-debt", 4.0), ("Hoang-debt", 6.3),
                       ("Quan-debt", 18.0)
                       ])

bill = data.reduceByKey(lambda key, value: key + value) # key = key thi value + value
print(f"Gia tri truoc khi bien doi: {bill.collect()}")
#bien doi du lieu cho tong no len dau
sortBill = bill.map(lambda x: (x[1], x[0])).sortByKey(ascending=False) # Them ascending = False de sap xep giam dan, mac dinh la ascending = True la se sap xep tang dan
# Du lieu dang o dang Tuple, muon chuyen doi du lieu sao cho tong no len dau, ten nguoi no xuong sau thi phai chuyen no thanh dang List
print(f"Gia tri sau khi bien doi: {sortBill.collect()}")


