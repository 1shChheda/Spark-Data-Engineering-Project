#Creating ML-ready features
##Builds feature store for various ML models

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as spark_sum
import os


class FeatureEngineering:
    def __init__(self, spark: SparkSession, base_path: str = "data"):
        self.spark = spark
        self.base_path = base_path
        self.silver_path = os.path.join(base_path, "silver")

    def create_customer_features(self):
        df = self.spark.read.format("delta") \
            .load(os.path.join(self.silver_path, "transactions_clean"))

        customer_features = df.groupBy("CustomerID").agg(
            count("InvoiceNo").alias("frequency"),
            spark_sum("total_price").alias("monetary"),
            avg("total_price").alias("avg_transaction_value")
        )

        customer_features.show(5)
        return customer_features


def main():
    from src.spark_session import create_spark_session
    spark = create_spark_session()
    FeatureEngineering(spark).create_customer_features()
    spark.stop()


if __name__ == "__main__":
    main()
