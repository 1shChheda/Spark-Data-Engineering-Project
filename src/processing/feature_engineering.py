#Creating ML-ready features
##Builds feature store for various ML models

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max,
    stddev, datediff, current_date, when, lag, log, sqrt
)
import os


class FeatureEngineering:

    def __init__(self, spark: SparkSession, base_path: str = "data"):
        self.spark = spark
        self.base_path = base_path
        self.silver_path = os.path.join(base_path, "silver")
        self.gold_path = os.path.join(base_path, "gold")
        self.feature_path = os.path.join(base_path, "feature_store")

    def create_customer_features(self):
        df_trans = self.spark.read.format("delta") \
            .load(os.path.join(self.silver_path, "transactions_clean"))

        df_customer_360 = self.spark.read.format("delta") \
            .load(os.path.join(self.gold_path, "customer_360"))

        df_valid = df_trans.filter(
            (col("CustomerID").isNotNull()) &
            (~col("is_return")) &
            (~col("is_cancellation"))
        )

        behavior_features = df_valid.groupBy("CustomerID").agg(
            count("InvoiceNo").alias("purchase_frequency_fe"),
            avg("total_price").alias("avg_transaction_value_fe"),
            stddev("total_price").alias("std_transaction_value")
        )

        window_customer = Window.partitionBy("CustomerID").orderBy("invoice_datetime")

        df_temporal = df_valid.withColumn(
            "prev_date", lag("invoice_datetime").over(window_customer)
        ).withColumn(
            "days_between", datediff(col("invoice_datetime"), col("prev_date"))
        )

        temporal_features = df_temporal.groupBy("CustomerID").agg(
            avg("days_between").alias("avg_days_between_purchases_fe"),
            stddev("days_between").alias("std_days_between_purchases")
        )

        engagement = df_valid.groupBy("CustomerID").agg(
            count("InvoiceNo").alias("total_invoices_fe"),
            datediff(current_date(), spark_max("invoice_datetime")).alias("recency_days_fe")
        )

        customer_features = df_customer_360 \
            .join(behavior_features, "CustomerID", "left") \
            .join(temporal_features, "CustomerID", "left") \
            .join(engagement, "CustomerID", "left")

        customer_features = customer_features.fillna(0)

        customer_features = customer_features.withColumn(
            "purchase_regularity",
            when(col("std_days_between_purchases") > 0,
                 col("avg_days_between_purchases_fe") / col("std_days_between_purchases"))
            .otherwise(0)
        )

        customer_features.write \
            .format("delta") \
            .mode("overwrite") \
            .save(os.path.join(self.feature_path, "customer_features"))

        return customer_features


def main():
    from src.spark_session import create_spark_session
    spark = create_spark_session()
    FeatureEngineering(spark).create_customer_features()
    spark.stop()


if __name__ == "__main__":
    main()
