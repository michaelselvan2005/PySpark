
import findspark
findspark.init()
from pyspark.sql import SparkSession
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()

df = spark.read.csv("train.csv", header=True, inferSchema=True)

df.createOrReplaceTempView("people")

result =spark.sql("select * from people")

result.show()
