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
    - Cast year fields and runtime to integers
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
        F.col("startYear").cast(StringType())
    ).withColumn(
        "endYear", 
        F.col("endYear").cast(StringType())
    ).withColumn(
        "runtimeMinutes", 
        F.col("runtimeMinutes").cast(IntegerType())
    )
    
    # For genres, we'll create two versions:
    # 1. A string version for the main table
    # 2. An exploded version that we'll load to a separate table
    
    # Create the main DataFrame with genres as string
    main_df = transformed_df
    
    # Create a separate DataFrame with exploded genres
    # This will have one row per genre per title
    exploded_df = transformed_df.select(
        "tconst",
        F.explode(
            F.when(
                F.col("genres").isNull() | (F.col("genres") == ""), 
                F.array(F.lit(None))
            ).otherwise(
                F.split(F.col("genres"), ",")
            )
        ).alias("genre")
    ).filter(F.col("genre").isNotNull())
    
    # Register temporary views for SQL operations
    main_df.createOrReplaceTempView("title_basics")
    exploded_df.createOrReplaceTempView("title_genres")
    
    # Print sample of exploded genres
    print("\nSample of exploded genres table:")
    exploded_df.show(10, truncate=False)
    
    print("\nTransformed Schema:")
    main_df.printSchema()
    print("\nTransformed sample data:")
    main_df.show(5, truncate=False)
    
    transformed_count = main_df.count()
    print(f"Transformed record count: {transformed_count}")
    
    validate_data(main_df)
    
    print(f"Writing main table to BigQuery: {output_table}")
    temp_bucket_name = temp_bucket.replace("gs://", "") if temp_bucket.startswith("gs://") else temp_bucket
    
    # Write main table to BigQuery
    main_df.write \
        .format("bigquery") \
        .option("table", output_table) \
        .option("temporaryGcsBucket", temp_bucket_name) \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_TRUNCATE") \
        .save()
    
    # Write exploded genres to a separate table
    genres_table = f"{output_table}_genres"
    print(f"Writing genres table to BigQuery: {genres_table}")
    
    exploded_df.write \
        .format("bigquery") \
        .option("table", genres_table) \
        .option("temporaryGcsBucket", temp_bucket_name) \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_TRUNCATE") \
        .save()
    
    print("Transformation completed successfully")
    print(f"To create an array field for genres, run the following SQL in BigQuery:")
    print(f"""
    CREATE OR REPLACE TABLE `{output_table}_with_arrays` AS
    SELECT 
        t.*,
        ARRAY_AGG(g.genre IGNORE NULLS) AS genres_array
    FROM 
        `{output_table}` t
    LEFT JOIN 
        `{genres_table}` g ON t.tconst = g.tconst
    GROUP BY 
        t.tconst, t.titleType, t.primaryTitle, t.originalTitle, 
        t.isAdult, t.startYear, t.endYear, t.runtimeMinutes, t.genres;
    """)

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