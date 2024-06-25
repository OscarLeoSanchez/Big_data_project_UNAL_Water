import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, year, month, dayofmonth
import os
from common.utils import configure_log

# Configuración del log
logger = configure_log("Bronze", "bronze.log")

findspark.init()


def process_bronze():
    try:
        logger.info("Starting bronze process")

        # Configuración de la sesión Spark
        spark = (
            SparkSession.builder.appName("ProcessBronze")
            .config("spark.sql.shuffle.partitions", "5")
            .config("spark.streaming.backpressure.enabled", "true")
            .config("spark.sql.streaming.checkpointLocation", "/unalwater/checkpoint")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "4g")
            .config(
                "spark.sql.streaming.stateStore.stateExpirationSecs", "86400"
            )  # 1 día
            .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
            .getOrCreate()
        )
        logger.info("Spark session configured successfully")

        log_dir = "/unalwater/logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            logger.info(f"Log directory {log_dir} created")

        data_raw = (
            spark.readStream.format("socket")
            .option("host", "unalwater_server")
            .option("port", 65432)
            .load()
        )
        logger.info("Data stream initialized from socket source")

        data_with_timestamp = data_raw.withColumn("timestamp", current_timestamp())
        data_partitioned = (
            data_with_timestamp.withColumn("year", year(col("timestamp")))
            .withColumn("month", month(col("timestamp")))
            .withColumn("day", dayofmonth(col("timestamp")))
        )
        logger.info("Data enriched with timestamp and partition columns")

        def batch_writer(batch_df, batch_id):
            try:
                if batch_df.count() > 0:
                    year_val = batch_df.select("year").first()["year"]
                    month_val = batch_df.select("month").first()["month"]
                    day_val = batch_df.select("day").first()["day"]

                    batch_df.coalesce(1).write.mode("append").parquet(
                        path=f"/unalwater/bronze/year={year_val}/month={month_val}/day={day_val}",
                        compression="snappy",
                    )
                    logger.info(
                        f"Batch {batch_id} written to /unalwater/bronze/year={year_val}/month={month_val}/day={day_val}"
                    )
                else:
                    logger.warning(f"Batch {batch_id} is empty. Skipping...")
            except Exception as e:
                logger.error(f"Error in batch {batch_id}: {str(e)}")

        query = (
            data_partitioned.writeStream.outputMode("append")
            .foreachBatch(batch_writer)
            .option("checkpointLocation", "/unalwater/checkpoint")
            .trigger(processingTime="2 minutes")
            .start()
        )
        logger.info("Streaming query configured and started")

        query.awaitTermination()

    except Exception as e:
        logger.error(f"An error occurred during the bronze process: {str(e)}")

    finally:
        logger.info("Processing finished")
        # Liberar recursos de Spark
        if "spark" in locals():
            spark.stop()
            logger.info("Spark session stopped")


process_bronze()
