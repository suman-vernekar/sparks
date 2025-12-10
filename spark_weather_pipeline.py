"""
Spark Weather Data Analysis Pipeline

This script implements a comprehensive weather data analysis pipeline with both
batch processing and real-time streaming capabilities using Apache Spark and Kafka.
"""

# Required imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
from pyspark.sql.window import Window
import json
import requests
import time

def create_spark_session():
    """Create and configure Spark session with Kafka support"""
    spark = SparkSession.builder \
        .appName("WeatherDataAnalysisPipeline") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    return spark

def read_weather_data_from_csv(spark, file_path):
    """
    Read weather data from CSV file
    
    Expected CSV format:
    date,min_temp,max_temp,avg_temp,humidity,wind_speed
    2023-01-01,2.5,10.5,6.5,75,12.3
    """
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("min_temp", DoubleType(), True),
        StructField("max_temp", DoubleType(), True),
        StructField("avg_temp", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True)
    ])
    
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def read_weather_data_from_api(api_url):
    """
    Read weather data from API endpoint
    Returns a DataFrame with weather data
    """
    # This is a placeholder - in practice, you would make API calls
    # and convert the response to a DataFrame
    response = requests.get(api_url)
    data = response.json()
    
    # Process API data and create DataFrame
    # Implementation depends on API response format
    pass

def send_data_to_kafka(df, kafka_bootstrap_servers, topic):
    """
    Send weather data to Kafka for real-time streaming
    """
    # Convert DataFrame to JSON format for Kafka
    df_json = df.select(to_json(struct(*df.columns)).alias("value"))
    
    # Write to Kafka
    df_json.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", topic) \
        .save()

def spark_streaming_consumer(spark, kafka_bootstrap_servers, topic):
    """
    Use Spark Streaming to consume live weather data from Kafka
    """
    # Read stream from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic) \
        .load()
    
    # Parse JSON data
    weather_schema = StructType([
        StructField("date", StringType(), True),
        StructField("min_temp", DoubleType(), True),
        StructField("max_temp", DoubleType(), True),
        StructField("avg_temp", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True)
    ])
    
    df_parsed = df.select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), weather_schema).alias("data")
    ).select("data.*")
    
    return df_parsed

def transform_and_clean_data(df):
    """
    Transform the data to clean and extract needed fields
    """
    # Convert date string to DateType
    df_cleaned = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    
    # Handle null values
    df_cleaned = df_cleaned.dropna()
    
    # Remove duplicates
    df_cleaned = df_cleaned.dropDuplicates()
    
    # Extract date components
    df_transformed = df_cleaned \
        .withColumn("year", year(col("date"))) \
        .withColumn("month", month(col("date"))) \
        .withColumn("day", dayofmonth(col("date")))
    
    return df_transformed

def compute_real_time_metrics(streaming_df):
    """
    Compute real-time min, max, and average weather metrics
    """
    # Define window for real-time processing
    window_duration = "10 minutes"
    sliding_interval = "5 minutes"
    
    # Compute metrics over sliding window
    metrics = streaming_df \
        .groupBy(window(col("timestamp"), window_duration, sliding_interval)) \
        .agg(
            min("min_temp").alias("min_temperature"),
            max("max_temp").alias("max_temperature"),
            avg("avg_temp").alias("avg_temperature"),
            avg("humidity").alias("avg_humidity"),
            avg("wind_speed").alias("avg_wind_speed")
        )
    
    return metrics

def store_streaming_results(metrics_df, output_path):
    """
    Store streaming results in a file or database
    """
    query = metrics_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", f"{output_path}/checkpoint") \
        .start()
    
    return query

def batch_processing_analysis(spark, data_path):
    """
    Use Spark Batch Processing to analyze the full dataset
    """
    # Read the complete dataset
    df = read_weather_data_from_csv(spark, data_path)
    
    # Transform and clean data
    df_transformed = transform_and_clean_data(df)
    
    # Compute comprehensive batch metrics
    batch_metrics = df_transformed.agg(
        min("min_temp").alias("overall_min_temp"),
        max("max_temp").alias("overall_max_temp"),
        avg("avg_temp").alias("overall_avg_temp"),
        min("humidity").alias("min_humidity"),
        max("humidity").alias("max_humidity"),
        avg("humidity").alias("avg_humidity"),
        min("wind_speed").alias("min_wind_speed"),
        max("wind_speed").alias("max_wind_speed"),
        avg("wind_speed").alias("avg_wind_speed")
    )
    
    # Yearly metrics
    yearly_metrics = df_transformed.groupBy("year").agg(
        min("min_temp").alias("min_temp"),
        max("max_temp").alias("max_temp"),
        avg("avg_temp").alias("avg_temp"),
        avg("humidity").alias("avg_humidity"),
        avg("wind_speed").alias("avg_wind_speed")
    ).orderBy("year")
    
    return batch_metrics, yearly_metrics

def compare_streaming_batch_metrics(streaming_metrics, batch_metrics):
    """
    Compare streaming metrics with batch metrics
    """
    # This function would implement the comparison logic
    # For now, we'll just return both metrics
    comparison = {
        "streaming_metrics": streaming_metrics,
        "batch_metrics": batch_metrics
    }
    return comparison

def output_final_results(batch_metrics, yearly_metrics):
    """
    Output final results and insights from both processing modes
    """
    print("=== BATCH PROCESSING RESULTS ===")
    print("\nOverall Weather Metrics:")
    batch_metrics.show()
    
    print("\nYearly Weather Trends:")
    yearly_metrics.show()
    
    # Generate insights
    print("\n=== KEY INSIGHTS ===")
    insights = yearly_metrics.collect()
    if insights:
        first_year = insights[0]
        last_year = insights[-1]
        
        temp_change = last_year["avg_temp"] - first_year["avg_temp"]
        if temp_change > 0:
            print(f"Temperature has increased by {temp_change:.2f}°C over the period")
        elif temp_change < 0:
            print(f"Temperature has decreased by {abs(temp_change):.2f}°C over the period")
        else:
            print("Temperature has remained stable over the period")
    
    print("\nAnalysis pipeline completed successfully!")

def main():
    """
    Main function to orchestrate the weather data analysis pipeline
    """
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Configuration
        WEATHER_DATA_PATH = "weather_data.csv"
        KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
        KAFKA_TOPIC = "weather-data"
        STREAMING_OUTPUT_PATH = "streaming_results"
        
        print("Starting Weather Data Analysis Pipeline...")
        
        # 1. Read weather data from CSV (or API)
        print("1. Reading weather data...")
        weather_df = read_weather_data_from_csv(spark, WEATHER_DATA_PATH)
        
        # 2. Send data to Kafka for real-time streaming
        print("2. Sending data to Kafka...")
        send_data_to_kafka(weather_df, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
        
        # 3. Use Spark Streaming to consume live weather data
        print("3. Starting streaming consumer...")
        streaming_df = spark_streaming_consumer(spark, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
        
        # 4. Transform the data to clean and extract needed fields
        print("4. Transforming and cleaning data...")
        cleaned_streaming_df = transform_and_clean_data(streaming_df)
        
        
        # 5. Compute real-time min, max, and average weather metrics
        print("5. Computing real-time metrics...")
        real_time_metrics = compute_real_time_metrics(cleaned_streaming_df)
        
        # 6. Store streaming results in a file
        print("6. Storing streaming results...")
        streaming_query = store_streaming_results(real_time_metrics, STREAMING_OUTPUT_PATH)
        
        # 7. Use Spark Batch Processing to analyze the full dataset
        print("7. Performing batch analysis...")
        batch_metrics, yearly_metrics = batch_processing_analysis(spark, WEATHER_DATA_PATH)
        
        # 8. Compare streaming metrics with batch metrics
        print("8. Comparing streaming and batch metrics...")
        comparison = compare_streaming_batch_metrics(real_time_metrics, batch_metrics)
        
        # 9. Output final results and insights
        print("9. Generating final results...")
        output_final_results(batch_metrics, yearly_metrics)
        
        # Wait for streaming to process some data
        time.sleep(30)
        streaming_query.stop()
        
    except Exception as e:
        print(f"Error in pipeline execution: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()