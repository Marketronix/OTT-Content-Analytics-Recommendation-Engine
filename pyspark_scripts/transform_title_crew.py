import argparse
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType

def create_spark_session():
    """Create a Spark session with BigQuery connector."""
    return (SparkSession.builder
            .appName("IMDb Title Crew Transformation")
            .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar")
            .getOrCreate())

def transform_title_crew(spark, input_file, output_table, temp_bucket):
    """
    Transform title_crew dataset:
    - Convert pipe-delimited directors and writers fields to arrays
    - Handle null markers
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
    
    # Apply transformations
    main_df = df
    
    # Create separate exploded tables for directors and writers
    directors_df = df.select(
        "tconst",
        F.explode(
            F.when(
                F.col("directors").isNull() | (F.col("directors") == ""), 
                F.array(F.lit(None))
            ).otherwise(
                F.split(F.col("directors"), ",")
            )
        ).alias("director_id")
    ).filter(F.col("director_id").isNotNull())
    
    writers_df = df.select(
        "tconst",
        F.explode(
            F.when(
                F.col("writers").isNull() | (F.col("writers") == ""), 
                F.array(F.lit(None))
            ).otherwise(
                F.split(F.col("writers"), ",")
            )
        ).alias("writer_id")
    ).filter(F.col("writer_id").isNotNull())
    
    # Register temporary views
    main_df.createOrReplaceTempView("title_crew")
    directors_df.createOrReplaceTempView("title_directors")
    writers_df.createOrReplaceTempView("title_writers")
    
    # Print samples
    print("\nSample of directors table:")
    directors_df.show(10, truncate=False)
    
    print("\nSample of writers table:")
    writers_df.show(10, truncate=False)
    
    print("\nTransformed Schema:")
    main_df.printSchema()
    
    transformed_count = main_df.count()
    print(f"Transformed record count: {transformed_count}")
    
    validate_data(main_df)
    
    print(f"Writing main table to BigQuery: {output_table}")
    temp_bucket_name = temp_bucket.replace("gs://", "") if temp_bucket.startswith("gs://") else temp_bucket
    
    # Write main table
    main_df.write \
        .format("bigquery") \
        .option("table", output_table) \
        .option("temporaryGcsBucket", temp_bucket_name) \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_TRUNCATE") \
        .save()
    
    # Write directors table
    directors_table = f"{output_table}_directors"
    print(f"Writing directors table to BigQuery: {directors_table}")
    
    directors_df.write \
        .format("bigquery") \
        .option("table", directors_table) \
        .option("temporaryGcsBucket", temp_bucket_name) \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_TRUNCATE") \
        .save()
    
    # Write writers table
    writers_table = f"{output_table}_writers"
    print(f"Writing writers table to BigQuery: {writers_table}")
    
    writers_df.write \
        .format("bigquery") \
        .option("table", writers_table) \
        .option("temporaryGcsBucket", temp_bucket_name) \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_TRUNCATE") \
        .save()
    
    print("Transformation completed successfully")
    print(f"To create an array field for directors and writers, run the following SQL in BigQuery:")
    print(f"""
    CREATE OR REPLACE TABLE `{output_table}_with_arrays` AS
    SELECT 
        t.*,
        CASE 
            WHEN COUNT(d.director_id) = 0 THEN []
            ELSE ARRAY_AGG(d.director_id)
        END AS directors_array,
        CASE 
            WHEN COUNT(w.writer_id) = 0 THEN []
            ELSE ARRAY_AGG(w.writer_id)
        END AS writers_array
    FROM 
        `{output_table}` t
    LEFT JOIN 
        `{directors_table}` d ON t.tconst = d.tconst
    LEFT JOIN 
        `{writers_table}` w ON t.tconst = w.tconst
    GROUP BY 
        t.tconst, t.directors, t.writers;
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
    
    # Check for primary key uniqueness
    total = df.count()
    distinct = df.select("tconst").distinct().count()
    duplicates = total - distinct
    print(f"  Duplicate tconst values: {duplicates}")
    
    if duplicates > 0:
        print("WARNING: Primary key constraint violated - duplicate tconst values found")

def parse_arguments():
    parser = argparse.ArgumentParser(description="Transform IMDb title_crew data")
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    parser.add_argument("--input_file", required=True, help="Input file path in GCS")
    parser.add_argument("--output_table", required=True, help="Output BigQuery table (project.dataset.table)")
    parser.add_argument("--temp_bucket", required=True, help="Temporary GCS bucket for BigQuery loading")
    return parser.parse_args()

def main():
    args = parse_arguments()
    spark = create_spark_session()
    try:
        transform_title_crew(spark, args.input_file, args.output_table, args.temp_bucket)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()