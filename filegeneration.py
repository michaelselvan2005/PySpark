import findspark
findspark.init()
from pyspark.sql import SparkSession
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()


# Create a DataFrame from a list of tuples
data = [("John", 25), ("Alice", 30), ("Bob", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Write the DataFrame to a CSV file

df.write.option("header", "true").csv("outputFile.csv")


# Stop the SparkSession
spark.stop()
