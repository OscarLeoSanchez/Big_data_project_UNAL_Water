import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, count, sum, max
from common.create_db import create_table
from common.utils import configure_log

# Configuración del log
logger = configure_log("Gold", "gold.log")

findspark.init()


def process_gold():
    try:
        logger.info("Starting gold process")
        spark = (
            SparkSession.builder.appName("ProcessGold")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "4g")
            # .config("spark.jars.packages", "org.postgresql:postgresql:42.3.6")
            # .config("spark.jars", "/opt/spark/jars/postgresql-42.2.5.jar")
            .getOrCreate()
        )

        try:
            last_processed_date_gold = (
                spark.read.parquet("/unalwater/gold/metadata")
                .select(max(col("processed_date")))
                .collect()[0][0]
            )
            logger.info("Last processed date for gold: %s", last_processed_date_gold)
        except Exception as e:
            last_processed_date_gold = "2024-01-01"
            logger.warning("Error reading last processed date for gold: %s", e)

        data = spark.read.parquet("/unalwater/silver").filter(
            col("processed_date") > last_processed_date_gold
        )
        logger.info(
            "Filtered data from silver to include records after %s",
            last_processed_date_gold,
        )

        data = data.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

        grouped_df = data.groupBy(
            "order_date", "comune", "neighborhood", "working_day"
        ).agg(
            sum("quantity_products").alias("total_quantity"),
            count("order_id").alias("order_count"),
            avg("time_delivery").alias("average_delivery_time"),
            avg("latitude").alias("latitude"),
            avg("longitude").alias("longitude"),
        )
        logger.info(
            "Grouped and aggregated data by order_date, comune, neighborhood, and working_day"
        )

        sorted_df = grouped_df.orderBy(col("order_date").asc(), col("comune").asc())
        reordered_df = sorted_df.select(
            "order_date",
            "neighborhood",
            "comune",
            "working_day",
            "total_quantity",
            "order_count",
            "average_delivery_time",
            "latitude",
            "longitude",
        )

        create_table()
        logger.info("Database table created or ensured to exist")

        reordered_df.write.jdbc(
            url="jdbc:postgresql://unalwater_db:5432/unalwater",
            table="daily_sales",
            mode="append",
            properties={
                "user": "user",
                "password": "root",
                "driver": "org.postgresql.Driver",
            },
        )
        logger.info("Data successfully written to the PostgreSQL database")
        logger.info(f"Processed {reordered_df.count()} registers")

        new_metadata = spark.createDataFrame(
            [(data.agg({"processed_date": "max"}).collect()[0][0],)], ["processed_date"]
        )
        new_metadata.write.mode("overwrite").parquet("/unalwater/gold/metadata")
        logger.info("Metadata successfully updated for gold layer")

    except Exception as e:
        logger.error("An error occurred during the gold process: %s", e)

    finally:
        logger.info("Processing to gold finished")
        if "spark" in locals():
            spark.stop()
            logger.info("Spark session stopped")


process_gold()
