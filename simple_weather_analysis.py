"""
Simple Weather Data Analysis Pipeline

This script implements a simplified weather data analysis pipeline using Apache Spark
with batch processing capabilities. This version can run without Kafka dependencies.
"""

# Required imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

def create_spark_session():
    """Create and configure Spark session"""
    spark = SparkSession.builder \
        .appName("SimpleWeatherDataAnalysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
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

def compute_batch_metrics(df):
    """
    Compute batch processing metrics for the full dataset
    """
    # Overall metrics
    overall_metrics = df.agg(
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
    
    # Monthly metrics
    monthly_metrics = df.groupBy("year", "month").agg(
        min("min_temp").alias("min_temp"),
        max("max_temp").alias("max_temp"),
        avg("avg_temp").alias("avg_temp"),
        avg("humidity").alias("avg_humidity"),
        avg("wind_speed").alias("avg_wind_speed"),
        count("*").alias("days_count")
    ).orderBy("year", "month")
    
    # Yearly metrics
    yearly_metrics = df.groupBy("year").agg(
        min("min_temp").alias("min_temp"),
        max("max_temp").alias("max_temp"),
        avg("avg_temp").alias("avg_temp"),
        avg("humidity").alias("avg_humidity"),
        avg("wind_speed").alias("avg_wind_speed"),
        count("*").alias("days_count")
    ).orderBy("year")
    
    return overall_metrics, monthly_metrics, yearly_metrics

def compute_temperature_trends(df):
    """
    Compute temperature trends using window functions
    """
    # Define window specification
    windowSpec = Window.partitionBy("year").orderBy("date")
    
    # Calculate daily temperature changes
    df_with_changes = df.withColumn(
        "temp_change", 
        col("avg_temp") - lag("avg_temp", 1).over(windowSpec)
    )
    
    # Find hottest and coldest days
    hottest_days = df.orderBy(col("max_temp").desc()).limit(5)
    coldest_days = df.orderBy(col("min_temp").asc()).limit(5)
    
    return df_with_changes, hottest_days, coldest_days

def output_results(overall_metrics, monthly_metrics, yearly_metrics, hottest_days, coldest_days):
    """
    Output final results and insights
    """
    print("=" * 50)
    print("WEATHER DATA ANALYSIS RESULTS")
    print("=" * 50)
    
    print("\n1. OVERALL WEATHER METRICS:")
    overall_metrics.show()
    
    print("\n2. YEARLY WEATHER TRENDS:")
    yearly_metrics.show()
    
    print("\n3. MONTHLY WEATHER PATTERNS:")
    monthly_metrics.show()
    
    print("\n4. HOTTEST DAYS RECORDED:")
    hottest_days.show()
    
    print("\n5. COLDEST DAYS RECORDED:")
    coldest_days.show()
    
    # Generate insights
    print("\n" + "=" * 50)
    print("KEY INSIGHTS")
    print("=" * 50)
    
    yearly_data = yearly_metrics.collect()
    if len(yearly_data) > 1:
        first_year = yearly_data[0]
        last_year = yearly_data[-1]
        
        temp_change = last_year["avg_temp"] - first_year["avg_temp"]
        if temp_change > 0:
            print(f"• Temperature has increased by {temp_change:.2f}°C from {first_year['year']} to {last_year['year']}")
        elif temp_change < 0:
            print(f"• Temperature has decreased by {abs(temp_change):.2f}°C from {first_year['year']} to {last_year['year']}")
        else:
            print(f"• Temperature has remained stable between {first_year['year']} and {last_year['year']}")
    
    # Find the year with highest average temperature
    max_temp_year = max(yearly_data, key=lambda x: x["avg_temp"])
    print(f"• The warmest year was {max_temp_year['year']} with an average temperature of {max_temp_year['avg_temp']:.2f}°C")
    
    # Find the year with lowest average temperature
    min_temp_year = min(yearly_data, key=lambda x: x["avg_temp"])
    print(f"• The coolest year was {min_temp_year['year']} with an average temperature of {min_temp_year['avg_temp']:.2f}°C")
    
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
        
        print("Starting Simple Weather Data Analysis Pipeline...")
        
        # 1. Read weather data from CSV
        print("1. Reading weather data...")
        weather_df = read_weather_data_from_csv(spark, WEATHER_DATA_PATH)
        print(f"   Loaded {weather_df.count()} records")
        
        # 2. Transform the data to clean and extract needed fields
        print("2. Transforming and cleaning data...")
        cleaned_df = transform_and_clean_data(weather_df)
        print(f"   After cleaning: {cleaned_df.count()} records")
        
        # 3. Compute batch metrics
        print("3. Computing batch metrics...")
        overall_metrics, monthly_metrics, yearly_metrics = compute_batch_metrics(cleaned_df)
        
        # 4. Compute temperature trends
        print("4. Analyzing temperature trends...")
        df_with_changes, hottest_days, coldest_days = compute_temperature_trends(cleaned_df)
        
        # 5. Output final results and insights
        print("5. Generating final results...")
        output_results(overall_metrics, monthly_metrics, yearly_metrics, hottest_days, coldest_days)
        
    except Exception as e:
        print(f"Error in pipeline execution: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()