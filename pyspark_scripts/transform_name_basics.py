import argparse
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, ArrayType, StringType

def create_spark_session():
    """Create a Spark session with BigQuery connector."""
    return (SparkSession.builder
            .appName("IMDb Name Basics Transformation")
            .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar")
            .getOrCreate())

def transform_name_basics(spark, input_file, output_table, temp_bucket):
    """
    Transform name_basics dataset:
    - Convert year fields to integers
    - Convert pipe-delimited fields to arrays
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
    main_df = df.withColumn(
        "birthYear", 
        F.col("birthYear").cast(IntegerType())
    ).withColumn(
        "deathYear", 
        F.col("deathYear").cast(IntegerType())
    )
    
    # Create separate exploded tables for professions and known titles
    professions_df = df.select(
        "nconst",
        F.explode(
            F.when(
                F.col("primaryProfession").isNull() | (F.col("primaryProfession") == ""), 
                F.array(F.lit(None))
            ).otherwise(
                F.split(F.col("primaryProfession"), ",")
            )
        ).alias("profession")
    ).filter(F.col("profession").isNotNull())
    
    known_titles_df = df.select(
        "nconst",
        F.explode(
            F.when(
                F.col("knownForTitles").isNull() | (F.col("knownForTitles") == ""), 
                F.array(F.lit(None))
            ).otherwise(
                F.split(F.col("knownForTitles"), ",")
            )
        ).alias("title_id")
    ).filter(F.col("title_id").isNotNull())
    
    # Register temporary views
    main_df.createOrReplaceTempView("name_basics")
    professions_df.createOrReplaceTempView("name_professions")
    known_titles_df.createOrReplaceTempView("name_known_titles")
    
    # Print samples
    print("\nSample of professions table:")
    professions_df.show(10, truncate=False)
    
    print("\nSample of known titles table:")
    known_titles_df.show(10, truncate=False)
    
    print("\nTransformed Schema:")
    main_df.printSchema()
    print("\nTransformed sample data:")
    main_df.show(5, truncate=False)
    
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
    
    # Write professions table
    professions_table = f"{output_table}_professions"
    print(f"Writing professions table to BigQuery: {professions_table}")
    
    professions_df.write \
        .format("bigquery") \
        .option("table", professions_table) \
        .option("temporaryGcsBucket", temp_bucket_name) \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_TRUNCATE") \
        .save()
    
    # Write known titles table
    known_titles_table = f"{output_table}_known_titles"
    print(f"Writing known titles table to BigQuery: {known_titles_table}")
    
    known_titles_df.write \
        .format("bigquery") \
        .option("table", known_titles_table) \
        .option("temporaryGcsBucket", temp_bucket_name) \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_TRUNCATE") \
        .save()
    
    print("Transformation completed successfully")
    print(f"To create array fields for professions and known titles, run the following SQL in BigQuery:")
    print(f"""
    CREATE OR REPLACE TABLE `{output_table}_with_arrays` AS
    SELECT 
        n.*,
        CASE 
            WHEN COUNT(p.profession) = 0 THEN []
            ELSE ARRAY_AGG(DISTINCT p.profession)
        END AS professions_array,
        CASE 
            WHEN COUNT(k.title_id) = 0 THEN []
            ELSE ARRAY_AGG(DISTINCT k.title_id)
        END AS known_titles_array
    FROM 
        `{output_table}` n
    LEFT JOIN 
        `{professions_table}` p ON n.nconst = p.nconst
    LEFT JOIN 
        `{known_titles_table}` k ON n.nconst = k.nconst
    GROUP BY 
        n.nconst, n.primaryName, n.birthYear, n.deathYear, n.primaryProfession, n.knownForTitles;
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
    
    # Check for birth/death year ranges
    year_stats = df.select(
        F.min("birthYear").alias("min_birth_year"),
        F.max("birthYear").alias("max_birth_year"),
        F.min("deathYear").alias("min_death_year"),
        F.max("deathYear").alias("max_death_year")
    ).collect()[0]
    
    print(f"  Birth year range: {year_stats['min_birth_year']} - {year_stats['max_birth_year']}")
    print(f"  Death year range: {year_stats['min_death_year']} - {year_stats['max_death_year']}")
    
    # Check for people with death before birth
    invalid_years = df.filter(
        (F.col("deathYear").isNotNull()) & 
        (F.col("birthYear").isNotNull()) & 
        (F.col("deathYear") < F.col("birthYear"))
    ).count()
    print(f"  People with death year before birth year: {invalid_years}")
    
    # Check for primary key uniqueness
    total = df.count()
    distinct = df.select("nconst").distinct().count()
    duplicates = total - distinct
    print(f"  Duplicate nconst values: {duplicates}")
    
    if duplicates > 0:
        print("WARNING: Primary key constraint violated - duplicate nconst values found")

def parse_arguments():
    parser = argparse.ArgumentParser(description="Transform IMDb name_basics data")
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    parser.add_argument("--input_file", required=True, help="Input file path in GCS")
    parser.add_argument("--output_table", required=True, help="Output BigQuery table (project.dataset.table)")
    parser.add_argument("--temp_bucket", required=True, help="Temporary GCS bucket for BigQuery loading")
    return parser.parse_args()

def main():
    args = parse_arguments()
    spark = create_spark_session()
    try:
        transform_name_basics(spark, args.input_file, args.output_table, args.temp_bucket)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()