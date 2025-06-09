import argparse
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, BooleanType, ArrayType, StringType

def create_spark_session():
    """Create a Spark session with BigQuery connector."""
    return (SparkSession.builder
            .appName("IMDb Title Basics Transformation")
            .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar")
            .getOrCreate())

def transform_title_basics(spark, input_file, output_table, temp_bucket):
    """
    Transform title_basics dataset:
    - Convert isAdult from "0"/"1" to boolean
    - Cast year fields and runtime to correct types
    - Split pipe-delimited genres into arrays
    - Handle null markers like "\\N"
    """
    # Read the input TSV file
    print(f"Reading input file: {input_file}")
    df = spark.read.option("sep", "\t").option("header", "true").option("nullValue", "\\N").csv(input_file)
    
    print("Original Schema:")
    df.printSchema()
    print("\nSample data:")
    df.show(5, truncate=False)
    
    original_count = df.count()
    print(f"Original record count: {original_count}")
    
    # Initial transformations
    transformed_df = df.withColumn(
        "isAdult", 
        F.when(F.col("isAdult") == "1", True)
         .when(F.col("isAdult") == "0", False)
         .otherwise(None)
         .cast(BooleanType())
    ).withColumn(
        "startYear", 
        F.when(F.col("startYear").isNull(), None).otherwise(F.col("startYear").cast(StringType()))
    ).withColumn(
        "endYear", 
        F.when(F.col("endYear").isNull(), None).otherwise(F.col("endYear").cast(StringType()))
    ).withColumn(
        "runtimeMinutes", 
        F.col("runtimeMinutes").cast(IntegerType())
    ).withColumn(
        "genres", 
        F.when(
            F.col("genres").isNull(), 
            F.array()
        ).otherwise(
            F.split(F.col("genres"), "\\|")
        )
    )
    
    # Explicitly cast 'genres' as Array of Strings
    transformed_df = transformed_df.withColumn(
        "genres", F.col("genres").cast(ArrayType(StringType()))
    )
    
    print("\nTransformed Schema:")
    transformed_df.printSchema()
    print("\nTransformed sample data:")
    transformed_df.show(5, truncate=False)
    
    transformed_count = transformed_df.count()
    print(f"Transformed record count: {transformed_count}")
    
    validate_data(transformed_df)
    
    print(f"Writing to BigQuery table: {output_table}")
    temp_bucket_name = temp_bucket.replace("gs://", "") if temp_bucket.startswith("gs://") else temp_bucket
    
    transformed_df.write \
        .format("bigquery") \
        .option("table", output_table) \
        .option("temporaryGcsBucket", temp_bucket_name) \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_TRUNCATE") \
        .save()

    print("Transformation completed successfully")

def validate_data(df):
    """Perform data validation checks."""
    print("\nNull checks:")
    null_counts = df.select([
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()
    
    for col, count in null_counts.items():
        print(f"  {col}: {count} nulls")
    
    print("\nYear range validation:")
    year_stats = df.select(
        F.min("startYear").alias("min_start_year"),
        F.max("startYear").alias("max_start_year"),
        F.min("endYear").alias("min_end_year"),
        F.max("endYear").alias("max_end_year")
    ).collect()[0]
    
    print(f"  Start year range: {year_stats['min_start_year']} - {year_stats['max_start_year']}")
    print(f"  End year range: {year_stats['min_end_year']} - {year_stats['max_end_year']}")
    
    current_year = 2025
    future_start_years = df.filter(F.col("startYear") > current_year).count()
    future_end_years = df.filter(
        (F.col("endYear").isNotNull()) & (F.col("endYear") > current_year)
    ).count()
    
    print(f"  Titles with future start years: {future_start_years}")
    print(f"  Titles with future end years: {future_end_years}")
    
    negative_runtime = df.filter(F.col("runtimeMinutes") < 0).count()
    print(f"  Titles with negative runtime: {negative_runtime}")
    
    total = df.count()
    distinct = df.select("tconst").distinct().count()
    duplicates = total - distinct
    print(f"  Duplicate tconst values: {duplicates}")
    
    if duplicates > 0:
        print("WARNING: Primary key constraint violated - duplicate tconst values found")

def parse_arguments():
    parser = argparse.ArgumentParser(description="Transform IMDb title_basics data")
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    parser.add_argument("--input_file", required=True, help="Input file path in GCS")
    parser.add_argument("--output_table", required=True, help="Output BigQuery table (project.dataset.table)")
    parser.add_argument("--temp_bucket", required=True, help="Temporary GCS bucket for BigQuery loading")
    return parser.parse_args()

def main():
    args = parse_arguments()
    spark = create_spark_session()
    try:
        transform_title_basics(spark, args.input_file, args.output_table, args.temp_bucket)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
