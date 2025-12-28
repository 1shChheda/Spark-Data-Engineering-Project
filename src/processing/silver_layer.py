#Data Cleaning & Validation Layer
##Transforms Bronze data into clean, validated Silver tables

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, when, lit, to_timestamp
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

        # basic cleaning
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
        
        silver_path = os.path.join(self.silver_path, "transactions_clean")
        df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)
        return df_clean

def main():
    from src.spark_session import create_spark_session
    spark = create_spark_session()
    
    silver = SilverLayer(spark)
    silver.clean_transactions()
    
    spark.stop()

if __name__ == "__main__":
    main()
