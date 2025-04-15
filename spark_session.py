from pyspark.sql import SparkSession

def get_spark_session():
    return (
        SparkSession.builder
        .appName("Iceberg e Delta")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2,io.delta:delta-core_2.12:2.4.0")
        .getOrCreate()
    )
