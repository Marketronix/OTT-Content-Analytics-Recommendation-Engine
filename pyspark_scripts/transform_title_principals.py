import argparse
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType

def create_spark_session():
    """Create a Spark session with BigQuery connector."""
    return (SparkSession.builder
            .appName("IMDb Title Principals Transformation")
            .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar")
            .getOrCreate())

def transform_title_principals(spark, input_file, output_table, temp_bucket):
    """
    Transform title_principals dataset:
    - Convert numeric fields to appropriate types
    - Handle null markers
    - Process characters field (may contain JSON-like data)
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
        "ordering", 
        F.col("ordering").cast(IntegerType())
    ).withColumn(
        "characters", 
        # Clean up characters field - often contains JSON-like "[\"Character Name\"]"
        F.when(
            F.col("characters").isNull(), 
            None
        ).otherwise(
            F.regexp_replace(F.col("characters"), "\\[|\\]|\"", "")
        )
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
    
    # Check for category distribution
    category_counts = df.groupBy("category").count().orderBy(F.desc("count"))
    print("\nCategory distribution:")
    category_counts.show(truncate=False)
    
    # Check for composite key uniqueness (tconst + ordering)
    total = df.count()
    distinct = df.select("tconst", "ordering").distinct().count()
    duplicates = total - distinct
    print(f"  Duplicate composite keys (tconst, ordering): {duplicates}")
    
    if duplicates > 0:
        print("WARNING: Composite key constraint violated - duplicate (tconst, ordering) values found")

def parse_arguments():
    parser = argparse.ArgumentParser(description="Transform IMDb title_principals data")
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    parser.add_argument("--input_file", required=True, help="Input file path in GCS")
    parser.add_argument("--output_table", required=True, help="Output BigQuery table (project.dataset.table)")
    parser.add_argument("--temp_bucket", required=True, help="Temporary GCS bucket for BigQuery loading")
    return parser.parse_args()

def main():
    args = parse_arguments()
    spark = create_spark_session()
    try:
        transform_title_principals(spark, args.input_file, args.output_table, args.temp_bucket)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()