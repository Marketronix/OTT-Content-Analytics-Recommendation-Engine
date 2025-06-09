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
    Transform title_basics dataset and load to BigQuery using a more direct approach
    with intermediate table and direct SQL to handle arrays properly.
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
    )
    
    # Process genres - create individual rows for each genre
    # This flattened approach will work more reliably with BigQuery loading
    print("\nCreating flattened genres table...")
    
    # First create a base table with all fields except genres
    base_table = transformed_df.select(
        "tconst", "titleType", "primaryTitle", "originalTitle", 
        "isAdult", "startYear", "endYear", "runtimeMinutes"
    )
    
    # Register as temp view for SQL
    base_table.createOrReplaceTempView("title_basics_base")
    
    # Create a separate genres table with tconst and genre
    genres_df = transformed_df.select(
        "tconst", 
        F.explode(
            F.when(F.col("genres").isNull() | (F.col("genres") == ""), F.array(F.lit(None)))
             .otherwise(F.split(F.col("genres"), ","))
        ).alias("genre")
    ).filter(F.col("genre").isNotNull())
    
    # Show sample of genres table
    print("\nSample of flattened genres table:")
    genres_df.show(10, truncate=False)
    
    # Register as temp view
    genres_df.createOrReplaceTempView("title_genres")
    
    # Save base table to BigQuery
    print(f"Writing base table to BigQuery: {output_table}")
    temp_bucket_name = temp_bucket.replace("gs://", "") if temp_bucket.startswith("gs://") else temp_bucket
    
    # Save base table to BigQuery
    base_table.write \
        .format("bigquery") \
        .option("table", output_table) \
        .option("temporaryGcsBucket", temp_bucket_name) \
        .option("writeDisposition", "WRITE_TRUNCATE") \
        .save()
    
    # Save genres to a separate staging table
    genres_staging_table = f"{output_table}_genres_staging"
    genres_df.write \
        .format("bigquery") \
        .option("table", genres_staging_table) \
        .option("temporaryGcsBucket", temp_bucket_name) \
        .option("writeDisposition", "WRITE_TRUNCATE") \
        .save()
    
    # Now use BigQuery SQL to merge the genres into the main table
    from google.cloud import bigquery
    client = bigquery.Client()
    
    # Extract dataset and table name from output_table
    project_dataset_table = output_table.split('.')
    if len(project_dataset_table) == 3:
        project, dataset, table = project_dataset_table
    elif len(project_dataset_table) == 2:
        project = client.project
        dataset, table = project_dataset_table
    else:
        raise ValueError(f"Invalid table name format: {output_table}")
    
    # Create a new table with the genres array using BigQuery SQL
    print("Creating final table with genres array using BigQuery SQL...")
    
    sql = f"""
    CREATE OR REPLACE TABLE `{project}.{dataset}.{table}_with_genres` AS
    SELECT 
        b.tconst, 
        b.titleType, 
        b.primaryTitle, 
        b.originalTitle, 
        b.isAdult, 
        b.startYear, 
        b.endYear, 
        b.runtimeMinutes,
        ARRAY_AGG(g.genre IGNORE NULLS) AS genres
    FROM 
        `{project}.{dataset}.{table}` b
    LEFT JOIN 
        `{project}.{dataset}.{table}_genres_staging` g
    ON 
        b.tconst = g.tconst
    GROUP BY 
        b.tconst, b.titleType, b.primaryTitle, b.originalTitle, 
        b.isAdult, b.startYear, b.endYear, b.runtimeMinutes
    """
    
    # Execute the SQL to create the final table
    query_job = client.query(sql)
    query_job.result()  # Wait for the query to complete
    
    # Now rename the _with_genres table to the original name
    sql_rename = f"""
    BEGIN
      DROP TABLE IF EXISTS `{project}.{dataset}.{table}_old`;
      ALTER TABLE `{project}.{dataset}.{table}` RENAME TO `{project}.{dataset}.{table}_old`;
      ALTER TABLE `{project}.{dataset}.{table}_with_genres` RENAME TO `{project}.{dataset}.{table}`;
    END;
    """
    
    # Execute the rename
    query_job = client.query(sql_rename)
    query_job.result()  # Wait for the query to complete
    
    # Clean up staging tables
    sql_cleanup = f"""
    BEGIN
      DROP TABLE IF EXISTS `{project}.{dataset}.{table}_old`;
      DROP TABLE IF EXISTS `{project}.{dataset}.{table}_genres_staging`;
    END;
    """
    
    # Execute the cleanup
    query_job = client.query(sql_cleanup)
    query_job.result()  # Wait for the query to complete
    
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