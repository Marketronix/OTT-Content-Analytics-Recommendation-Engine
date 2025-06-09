import argparse
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType

def create_spark_session():
    """Create a Spark session with BigQuery connector."""
    return (SparkSession.builder
            .appName("IMDb Title Ratings Transformation")
            .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar")
            .getOrCreate())

def transform_title_ratings(spark, input_file, output_table, temp_bucket):
    """
    Transform title_ratings dataset:
    - Convert numeric fields to appropriate types
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
    transformed_df = df.withColumn(
        "averageRating", 
        F.col("averageRating").cast(FloatType())
    ).withColumn(
        "numVotes", 
        F.col("numVotes").cast(IntegerType())
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
    
    # Write to BigQuery
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
    
    # Check for invalid ratings
    invalid_ratings = df.filter(
        (F.col("averageRating") < 0) | (F.col("averageRating") > 10)
    ).count()
    print(f"  Invalid ratings (outside 0-10): {invalid_ratings}")
    
    # Check for negative vote counts
    negative_votes = df.filter(F.col("numVotes") < 0).count()
    print(f"  Negative vote counts: {negative_votes}")
    
    # Check for primary key uniqueness
    total = df.count()
    distinct = df.select("tconst").distinct().count()
    duplicates = total - distinct
    print(f"  Duplicate tconst values: {duplicates}")
    
    if duplicates > 0:
        print("WARNING: Primary key constraint violated - duplicate tconst values found")

def parse_arguments():
    parser = argparse.ArgumentParser(description="Transform IMDb title_ratings data")
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    parser.add_argument("--input_file", required=True, help="Input file path in GCS")
    parser.add_argument("--output_table", required=True, help="Output BigQuery table (project.dataset.table)")
    parser.add_argument("--temp_bucket", required=True, help="Temporary GCS bucket for BigQuery loading")
    return parser.parse_args()

def main():
    args = parse_arguments()
    spark = create_spark_session()
    try:
        transform_title_ratings(spark, args.input_file, args.output_table, args.temp_bucket)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()