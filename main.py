import findspark
findspark.init()
from pyspark.sql import SparkSession

# Initialize SparkSession


spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()



# Create a DataFrame from a list of tuples
data = [("John", 25), ("Alice", 30), ("Bob", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Show the DataFrame
df.show()

# Filter the DataFrame
filtered_df = df.filter(df["Age"] > 22)
filtered_df.show()

# Stop the SparkSession
spark.stop()