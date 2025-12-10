# Final test script to verify the environment
print("Testing Python environment...")

try:
    import pyspark
    print("✓ PySpark imported successfully")
    
    from pyspark.sql import SparkSession
    print("✓ SparkSession imported successfully")
    
    # Create a minimal Spark session
    spark = SparkSession.builder.appName("FinalTest").master("local[1]").getOrCreate()
    print("✓ Spark session created successfully")
    
    # Create a simple DataFrame
    data = [("2023-01-01", 10.5), ("2023-01-02", 12.3)]
    df = spark.createDataFrame(data, ["date", "temperature"])
    print("✓ DataFrame created successfully")
    
    # Show the DataFrame
    df.show()
    print("✓ DataFrame displayed successfully")
    
    # Stop Spark session
    spark.stop()
    print("✓ Spark session stopped successfully")
    
    print("\n🎉 All tests passed! Environment is ready for weather analysis.")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()