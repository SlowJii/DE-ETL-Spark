from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import LongType, StringType, StructType, StructField, IntegerType, FloatType, MapType, ArrayType, \
    DateType, TimestampType, BooleanType
from datetime import datetime


spark = SparkSession.builder \
    .appName("SlowJii") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") .getOrCreate()

"""
                                PYSPARK SQL TYPE

===========BASIC================
StringType = Kieu chuoi ki tu
LongType = Kieu so nguyen 64 bit
IntegerType = Kieu so nguyen 32 bit
FloatType = Kieu so thuc 32 bit
DoubleType = Kieu so thuc 64 bit
BooleanType = Kieu logical
TimestampType = Kieu thoi gian ngay va gio
DateType = YYYY/MM/DD
DecimalType(precision, scale) = Do chinh xac cua so thap phan
    - precision: Tong chu so
    - scale: So chu so sau dau phay
ByteType = Kieu so nguyen 8 bit
ShortType = Kieu so nguyen 16 bit

==============ADVANCED==================
StructType = Bieu dien mot cau truc du lieu
StrucField(name, datatype, nullable) = Bieu dien mot truong du lieu trong StructType
    - name: Ten ban ghi (record name)
    - datatype: Kieu du lieu
    - nullable: True-False (Kha nang cot nay co dc phep NULL hay khong)
ArrayType(elementType) = Bieu dien cac mang duoc chi dinh
    - elementType: Kieu du lieu cua cac phan tu ben trong mang
MapType(keyType, valueType) = Bieu dien cac cap Key-Value 
    - keyType: Kieu du lieu cua khoa (key)
    - valueType: Kieu du lieu cua gia tri (value)

"""


data = [
    Row(
        id = 1,
        name = "Le Bao Hoang",
        age = 23,
        salary = 1000.0,
        bonus = 108.9,
        score = [1,8,9],
        attributes = {"department": "CS2", "role": "Entry Fragger"},
        hire_date = datetime.strptime("2025-01-01", "%Y-%m-%d").date(),
        last_login = datetime.strptime("2025-02-02 10:18:40", "%Y-%m-%d %H:%M:%S"),
        isActive = False
    ),
    Row(
        id = 2,
        name = "Dang Tien Dat",
        age = 25,
        salary = 10000.00,
        bonus = 2000.00,
        score = [10,9,9],
        attributes = {"department": "CS2", "role": "In Game Leader"},
        hire_date = datetime.strptime("2025-01-01", "%Y-%m-%d").date(),
        last_login = datetime.strptime("2025-02-02 10:18:40", "%Y-%m-%d %H:%M:%S"),
        isActive = True
    )
]

schema = StructType([
    StructField("id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary",FloatType(), True ),
    StructField("bonus", FloatType(), True),
    StructField("score", ArrayType(IntegerType()), True),
    StructField("attributes", MapType(StringType(), StringType()), True),
    StructField("hire_date", DateType(), True),
    StructField("last_login", TimestampType(), True),
    StructField("isActive", BooleanType(), True)
])


df = spark.createDataFrame(data,schema)
df.show(truncate=False) #truncate = False: Show het cac dong du lieu
df.printSchema()


df1 = spark.range(1,10,2).toDF("nums").show()