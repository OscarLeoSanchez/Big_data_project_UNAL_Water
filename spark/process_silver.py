import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    current_timestamp,
    hour,
    minute,
    second,
    unix_timestamp,
    avg,
    udf,
    explode,
    split,
    max,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
    Row,
)
from process.poligon_finder import PolygonFinder
import os
import logging
from common.utils import configure_log

# Configuración del log
logger = configure_log("Silver", "silver.log")

findspark.init()
spark = (
    SparkSession.builder.appName("ProcessSilver")
    .config("spark.sql.shuffle.partitions", "5")
    .getOrCreate()
)


def find_neighborhood(lat, lon):
    logger.info("Finding neighborhood for coordinates: (%.4f, %.4f)", lat, lon)
    path_parquet = "data.base/medellin_neighborhoods.parquet"
    polygon_finder = PolygonFinder(path_parquet)
    neighborhood, comune = polygon_finder.get_neighbord_from_coordinates(lat, lon)
    return Row(neighborhood=neighborhood, comune=comune)


schema_neighborhood = StructType(
    [
        StructField("neighborhood", StringType(), True),
        StructField("comune", StringType(), True),
    ]
)
find_neighborhood_udf = udf(find_neighborhood, schema_neighborhood)


def add_neighborhood(df):
    logger.info("Adding neighborhood information to DataFrame")
    df = df.withColumn(
        "output_col", find_neighborhood_udf(col("latitude"), col("longitude"))
    )
    df = df.withColumn("neighborhood", col("output_col").getItem("neighborhood"))
    df = df.withColumn("comune", col("output_col").getItem("comune"))
    df = df.drop("output_col")
    return df


def process_silver():
    logger.info("Starting silver process")

    absolute_customers_path = os.path.abspath("data.base/customers.parquet")
    absolute_employees_path = os.path.abspath("data.base/employees.parquet")

    try:
        schema = StructType(
            [
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("order_date", TimestampType(), True),
                StructField("dispatch_date", TimestampType(), True),
                StructField("delivery_date", TimestampType(), True),
                StructField("customer_id", IntegerType(), True),
                StructField("employee_id", IntegerType(), True),
                StructField("quantity_products", IntegerType(), True),
                StructField("order_id", StringType(), True),
            ]
        )

        try:
            last_processed_date = (
                spark.read.parquet("/unalwater/silver/metadata")
                .select(max(col("processed_date")))
                .collect()[0][0]
            )
            logger.info("Last processed date: %s", last_processed_date)
        except Exception as e:
            last_processed_date = "2024-01-01"
            logger.warning("Error reading last processed date: %s", e)

        data = spark.read.parquet("/unalwater/bronze")

        if "timestamp" not in data.columns:
            logger.error("Column 'timestamp' not found in bronze data")
            return

        data = data.filter(col("timestamp") > last_processed_date)
        logger.info(
            "Filtered data to include only new records after %s", last_processed_date
        )

        data = data.filter(col("value").isNotNull()).dropDuplicates()
        json_df = data.selectExpr("CAST(value AS STRING) as json_values")
        datos = json_df.withColumn(
            "json_data", from_json(col("json_values"), schema)
        ).select("json_data.*")
        datos = datos.dropna()
        logger.info("Data cleaned and null values dropped")

        datos = datos.withColumn("order_hour", hour(col("order_date")))
        datos = datos.withColumn("order_minute", minute(col("order_date")))
        datos = datos.withColumn(
            "time_delivery",
            (unix_timestamp("delivery_date") - unix_timestamp("dispatch_date")) / 60,
        )

        def classify_working_day(hora):
            return "M" if hora < 12 else "T"

        classify_working_day_udf = udf(classify_working_day)

        datos = datos.withColumn("working_day", classify_working_day_udf("order_hour"))
        datos = datos.withColumn("processed_date", current_timestamp())

        customers_data = spark.read.parquet(f"file://{absolute_customers_path}")
        employees_data = spark.read.parquet(f"file://{absolute_employees_path}")
        datos_con_nombres = datos.join(
            employees_data.select(col("employee_id"), col("name")),
            datos.employee_id == employees_data.employee_id,
            "left",
        )
        datos_con_nombres = datos_con_nombres.withColumnRenamed("name", "employee_name")
        datos_con_nombres = datos_con_nombres.select(datos["*"], col("employee_name"))

        datos = datos_con_nombres.join(
            customers_data.select(col("customer_id"), col("name")),
            datos_con_nombres.customer_id == customers_data.customer_id,
            "left",
        )
        datos = datos.withColumnRenamed("name", "customer_name")
        datos = datos.select(datos_con_nombres["*"], col("customer_name"))

        datos = add_neighborhood(datos)

        datos.write.parquet("/unalwater/silver", mode="append")
        logger.info("Data successfully written to /unalwater/silver")

        logger.info(f"Processed {datos.count()} registers")

        new_metadata = spark.createDataFrame(
            [(datos.agg({"processed_date": "max"}).collect()[0][0],)],
            ["processed_date"],
        )
        new_metadata.write.parquet("/unalwater/silver/metadata", mode="overwrite")
        logger.info("Metadata successfully updated")

    except Exception as e:
        logger.error("An error occurred during the silver process: %s", e)

    finally:
        logger.info("Processing to silver finished...")
        if "spark" in locals():
            spark.stop()
            logger.info("Spark session stopped")


process_silver()
