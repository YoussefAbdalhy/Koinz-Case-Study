import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# Initialize Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PostgresToClickHouse")

def main():
    spark = (
        SparkSession.builder
        .appName("postgres-to-clickhouse-incremental")
        .getOrCreate()
    )

# ClickHouse connnection details
    clickhouse_jdbc_url = "jdbc:clickhouse://clickhouse:8123/default"
    clickhouse_properties = {
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "batchsize": "100000", 
        "rewriteBatchedStatements": "true"
    }

# Get watermark from ClickHouse
    watermark_query = "(SELECT max(updated_at) AS last_max FROM app_user_visits_fact) AS t"
    
    try:
        watermark_df = spark.read.jdbc(
            url=clickhouse_jdbc_url,
            table=watermark_query,
            properties=clickhouse_properties
        )
        last_max = watermark_df.collect()[0]["last_max"]
    except Exception as e:
        logger.warning(f"Could not fetch watermark: {e}")
        last_max = None

    if last_max is None:
        start_epoch_ms = 0
    else:
        start_time = datetime.fromtimestamp(last_max / 1000) - timedelta(minutes=15)
        start_epoch_ms = int(start_time.timestamp() * 1000)

    postgres_jdbc_url = "jdbc:postgresql://postgres:5432/app_db"
    
    current_max_ms = int(datetime.now().timestamp() * 1000)
    
    postgres_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver",
        "partitionColumn": "updated_at",
        "lowerBound": str(start_epoch_ms),
        "upperBound": str(current_max_ms),
        "numPartitions": "4" 
    }

    postgres_query = f"(SELECT * FROM app_user_visits_fact WHERE updated_at >= {start_epoch_ms}) AS src"

    source_df = spark.read.jdbc(
        url=postgres_jdbc_url,
        table=postgres_query,
        properties=postgres_properties
    )

    transformed_df = (
        source_df
        .withColumn("is_deleted", col("is_deleted").cast("byte"))
        .withColumn("is_fraud", col("is_fraud").cast("byte"))
    )

    window_spec = Window.partitionBy("id").orderBy(col("updated_at").desc())
    dedup_df = (
        transformed_df
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    
    try:
        dedup_df.write \
            .mode("append") \
            .jdbc(
                url=clickhouse_jdbc_url,
                table="app_user_visits_fact",
                properties=clickhouse_properties
            )
        
        # Log results for Airflow visibility
        logger.info("ETL process completed successfully.")
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise

    spark.stop()

if __name__ == "__main__":
    main()