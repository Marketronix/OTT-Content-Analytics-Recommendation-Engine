import argparse
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, BooleanType, ArrayType, StringType

def create_spark_session():
    """Create a Spark session with BigQuery connector."""
    return (SparkSession.builder
            .appName("IMDb Title AKAs Transformation")
            .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar")
            .getOrCreate())

def transform_title_akas(spark, input_file, output_table, temp_bucket):
    """
    Transform title_akas dataset:
    - Convert isOriginalTitle from "0"/"1" to boolean
    - Convert ordering to integer
    - Handle types and attributes arrays
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
        "ordering", 
        F.col("ordering").cast(IntegerType())
    ).withColumn(
        "isOriginalTitle", 
        F.when(F.col("isOriginalTitle") == "1", True)
         .when(F.col("isOriginalTitle") == "0", False)
         .otherwise(None)
         .cast(BooleanType())
    )
    
    # Create separate tables for types and attributes
    types_df = df.select(
        "titleId",
        "ordering",
        F.explode(
            F.when(
                F.col("types").isNull() | (F.col("types") == ""), 
                F.array(F.lit(None))
            ).otherwise(
                F.split(F.col("types"), ",")
            )
        ).alias("type")
    ).filter(F.col("type").isNotNull())
    
    attributes_df = df.select(
        "titleId",
        "ordering",
        F.explode(
            F.when(
                F.col("attributes").isNull() | (F.col("attributes") == ""), 
                F.array(F.lit(None))
            ).otherwise(
                F.split(F.col("attributes"), ",")
            )
        ).alias("attribute")
    ).filter(F.col("attribute").isNotNull())
    
    # Register temporary views
    main_df.createOrReplaceTempView("title_akas")
    types_df.createOrReplaceTempView("title_akas_types")
    attributes_df.createOrReplaceTempView("title_akas_attributes")
    
    # Print samples
    print("\nSample of types table:")
    types_df.show(10, truncate=False)
    
    print("\nSample of attributes table:")
    attributes_df.show(10, truncate=False)
    
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
    
    # Write types table
    types_table = f"{output_table}_types"
    print(f"Writing types table to BigQuery: {types_table}")
    
    types_df.write \
        .format("bigquery") \
        .option("table", types_table) \
        .option("temporaryGcsBucket", temp_bucket_name) \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_TRUNCATE") \
        .save()
    
    # Write attributes table
    attributes_table = f"{output_table}_attributes"
    print(f"Writing attributes table to BigQuery: {attributes_table}")
    
    attributes_df.write \
        .format("bigquery") \
        .option("table", attributes_table) \
        .option("temporaryGcsBucket", temp_bucket_name) \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_TRUNCATE") \
        .save()
    
    print("Transformation completed successfully")
    print(f"To create array fields for types and attributes, run the following SQL in BigQuery:")
    print(f"""
    CREATE OR REPLACE TABLE `{output_table}_with_arrays` AS
    SELECT 
        a.*,
        CASE 
            WHEN COUNT(t.type) = 0 THEN []
            ELSE ARRAY_AGG(t.type)
        END AS types_array,
        CASE 
            WHEN COUNT(att.attribute) = 0 THEN []
            ELSE ARRAY_AGG(att.attribute)
        END AS attributes_array
    FROM 
        `{output_table}` a
    LEFT JOIN 
        `{types_table}` t ON a.titleId = t.titleId AND a.ordering = t.ordering
    LEFT JOIN 
        `{attributes_table}` att ON a.titleId = att.titleId AND a.ordering = att.ordering
    GROUP BY 
        a.titleId, a.ordering, a.title, a.region, a.language, a.types, a.attributes, a.isOriginalTitle;
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
    
    # Check for region distribution
    region_counts = df.groupBy("region").count().orderBy(F.desc("count"))
    print("\nTop 10 regions:")
    region_counts.show(10, truncate=False)
    
    # Check for language distribution
    language_counts = df.groupBy("language").count().orderBy(F.desc("count"))
    print("\nTop 10 languages:")
    language_counts.show(10, truncate=False)
    
    # Check for composite key uniqueness (titleId + ordering)
    total = df.count()
    distinct = df.select("titleId", "ordering").distinct().count()
    duplicates = total - distinct
    print(f"  Duplicate composite keys (titleId, ordering): {duplicates}")
    
    if duplicates > 0:
        print("WARNING: Composite key constraint violated - duplicate (titleId, ordering) values found")

def parse_arguments():
    parser = argparse.ArgumentParser(description="Transform IMDb title_akas data")
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    parser.add_argument("--input_file", required=True, help="Input file path in GCS")
    parser.add_argument("--output_table", required=True, help="Output BigQuery table (project.dataset.table)")
    parser.add_argument("--temp_bucket", required=True, help="Temporary GCS bucket for BigQuery loading")
    return parser.parse_args()

def main():
    args = parse_arguments()
    spark = create_spark_session()
    try:
        transform_title_akas(spark, args.input_file, args.output_table, args.temp_bucket)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()