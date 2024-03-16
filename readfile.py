import findspark
findspark.init()
from pyspark.sql import SparkSession
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()

# Read CSV file into a DataFrame
df = spark.read.csv("train.csv", header=True, inferSchema=True).createOrReplaceTempView("Zipcodes1")

          

spark.sql("select case_id,continent,education_of_employee,has_job_experience from Zipcodes1").write.mode("overwrite").csv("Zipcodes_Output1.csv")

spark.stop()