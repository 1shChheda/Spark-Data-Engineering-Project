#Data Cleaning & Validation Layer
##Transforms Bronze data into clean, validated Silver tables

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, trim, upper, when, lit, to_timestamp,
    year, month, dayofmonth, dayofweek, hour, row_number
)
import os


class SilverLayer:
    def __init__(self, spark: SparkSession, base_path: str = "data"):
        self.spark = spark
        self.base_path = base_path
        self.bronze_path = os.path.join(base_path, "bronze")
        self.silver_path = os.path.join(base_path, "silver")

    def clean_transactions(self):
        df = self.spark.read.format("delta") \
            .load(os.path.join(self.bronze_path, "transactions"))

        df_clean = df \
            .filter(col("Quantity").isNotNull()) \
            .filter(col("UnitPrice").isNotNull()) \
            .filter(col("UnitPrice") >= 0) \
            .withColumn("StockCode", trim(upper(col("StockCode")))) \
            .withColumn("Country", trim(col("Country"))) \
            .withColumn("Description",
                        when(col("Description").isNull(), "UNKNOWN")
                        .otherwise(trim(col("Description")))) \
            .withColumn("invoice_datetime", to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm"))

        #derived columns
        df_enriched = df_clean \
            .withColumn("total_price", col("Quantity") * col("UnitPrice")) \
            .withColumn("is_return", when(col("Quantity") < 0, lit(True)).otherwise(lit(False))) \
            .withColumn("is_cancellation",
                        when(col("InvoiceNo").startswith("C"), lit(True)).otherwise(lit(False))) \
            .withColumn("year", year(col("invoice_datetime"))) \
            .withColumn("month", month(col("invoice_datetime"))) \
            .withColumn("day", dayofmonth(col("invoice_datetime"))) \
            .withColumn("day_of_week", dayofweek(col("invoice_datetime"))) \
            .withColumn("hour", hour(col("invoice_datetime")))

        #deduplication
        window = Window.partitionBy("record_hash") \
                       .orderBy(col("ingestion_timestamp").desc())

        df_deduped = df_enriched \
            .withColumn("row_num", row_number().over(window)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")

        silver_path = os.path.join(self.silver_path, "transactions_clean")
        df_deduped.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(silver_path)

        return df_deduped


def main():
    from src.spark_session import create_spark_session
    spark = create_spark_session()

    SilverLayer(spark).clean_transactions()
    spark.stop()


if __name__ == "__main__":
    main()
