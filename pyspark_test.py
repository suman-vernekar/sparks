from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("PySparkTest") \
    .master("local[*]") \
    .getOrCreate()

# Create a simple DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
print("Simple PySpark Test:")
df.show()

# Stop Spark session
spark.stop()

print("PySpark is working correctly!")