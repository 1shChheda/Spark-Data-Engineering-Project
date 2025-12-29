#Data Cleaning & Validation Layer
##Transforms Bronze data into clean, validated Silver tables

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, trim, upper, regexp_replace, to_timestamp,
    coalesce, lit, row_number, count, sum as spark_sum, avg, 
    min as spark_min, max as spark_max, datediff, current_date,
    year, month, dayofmonth, dayofweek, hour, date_format
)
from pyspark.sql.types import DoubleType, IntegerType
from delta import DeltaTable
import os


class SilverLayer:
    #Handles data cleaning and validation for Silver layer
    
    def __init__(self, spark: SparkSession, base_path: str = "data"):
        self.spark = spark
        self.base_path = base_path
        self.bronze_path = os.path.join(base_path, "bronze")
        self.silver_path = os.path.join(base_path, "silver")
        
    def clean_transactions(self):
        #clean and validate transaction data
        print("\n" + "="*60)
        print("SILVER LAYER: Cleaning Transactions")
        print("="*60)
        
        #read from Bronze layer
        df_bronze = self.spark.read.format("delta") \
            .load(os.path.join(self.bronze_path, "transactions"))
        
        print(f"Bronze records: {df_bronze.count():,}")
        
        ##step 1: Parse dates
        df = df_bronze.withColumn(
            "invoice_datetime",
            to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm")
        )
        
        ##step 2: Clean and validate data
        df_clean = df \
            .filter(col("invoice_datetime").isNotNull()) \
            .filter(col("Quantity").isNotNull()) \
            .filter(col("UnitPrice").isNotNull()) \
            .filter(col("UnitPrice") >= 0) \
            .withColumn("StockCode", trim(upper(col("StockCode")))) \
            .withColumn("Country", trim(col("Country"))) \
            .withColumn("Description", 
                       when(col("Description").isNull(), "UNKNOWN")
                       .otherwise(trim(col("Description")))) \
            .withColumn("CustomerID",
                       when(col("CustomerID").isNull(), lit(None))
                       .otherwise(col("CustomerID").cast("integer")))
        
        ##step 3: Add derived columns
        df_enriched = df_clean \
            .withColumn("total_price", col("Quantity") * col("UnitPrice")) \
            .withColumn("is_return", when(col("Quantity") < 0, lit(True)).otherwise(lit(False))) \
            .withColumn("is_cancellation", 
                       when(col("InvoiceNo").startswith("C"), lit(True)).otherwise(lit(False))) \
            .withColumn("year", year(col("invoice_datetime"))) \
            .withColumn("month", month(col("invoice_datetime"))) \
            .withColumn("day", dayofmonth(col("invoice_datetime"))) \
            .withColumn("day_of_week", dayofweek(col("invoice_datetime"))) \
            .withColumn("hour", hour(col("invoice_datetime"))) \
            .withColumn("quarter", 
                       when(col("month").isin(1,2,3), 1)
                       .when(col("month").isin(4,5,6), 2)
                       .when(col("month").isin(7,8,9), 3)
                       .otherwise(4)) \
            .withColumn("is_weekend", 
                       when(col("day_of_week").isin(1, 7), lit(True)).otherwise(lit(False)))
        
        ##step 4: Remove duplicates
        window = Window.partitionBy("record_hash").orderBy(col("ingestion_timestamp").desc())
        df_deduped = df_enriched \
            .withColumn("row_num", row_number().over(window)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")
        
        print(f"After cleaning: {df_deduped.count():,} records")
        
        ##step 5: write to Silver
        silver_path = os.path.join(self.silver_path, "transactions_clean")
        df_deduped.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(silver_path)
        
        print(f"✓ Written to {silver_path}")
        
        #validation
        self._validate_silver_transactions(df_deduped)
        
        return df_deduped
    
    def create_customer_profiles(self):
        #create customer dimension from transactions
        print("\n" + "="*60)
        print("SILVER LAYER: Creating Customer Profiles")
        print("="*60)
        
        #read clean transactions
        df_trans = self.spark.read.format("delta") \
            .load(os.path.join(self.silver_path, "transactions_clean"))
        
        #filter valid customers only
        df_customers = df_trans \
            .filter(col("CustomerID").isNotNull()) \
            .filter(~col("is_return")) \
            .filter(~col("is_cancellation"))
        
        #aggregate customer profile
        customer_profile = df_customers.groupBy("CustomerID", "Country").agg(
            spark_min("invoice_datetime").alias("first_purchase_date"),
            spark_max("invoice_datetime").alias("last_purchase_date"),
            count("InvoiceNo").alias("total_transactions"),
            spark_sum("total_price").alias("total_spent"),
            avg("total_price").alias("avg_transaction_value"),
            count(col("InvoiceNo")).alias("unique_products_purchased")
        )
        
        #add derived metrics
        customer_profile = customer_profile \
            .withColumn("customer_lifetime_days", 
                       datediff(col("last_purchase_date"), col("first_purchase_date"))) \
            .withColumn("days_since_last_purchase",
                       datediff(current_date(), col("last_purchase_date"))) \
            .withColumn("avg_days_between_purchases",
                       col("customer_lifetime_days") / col("total_transactions"))
        
        #write to Silver
        silver_path = os.path.join(self.silver_path, "customers")
        customer_profile.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(silver_path)
        
        print(f"✓ Created {customer_profile.count():,} customer profiles")
        print(f"✓ Written to {silver_path}")
        
        customer_profile.show(10, truncate=False)
        
        return customer_profile
    
    def create_product_catalog(self):
        #create product dimension from transactions
        print("\n" + "="*60)
        print("SILVER LAYER: Creating Product Catalog")
        print("="*60)
        
        #read clean transactions
        df_trans = self.spark.read.format("delta") \
            .load(os.path.join(self.silver_path, "transactions_clean"))
        
        #filter valid products
        df_products = df_trans \
            .filter(~col("is_return")) \
            .filter(~col("is_cancellation"))
        
        #aggregate product metrics
        product_catalog = df_products.groupBy("StockCode", "Description").agg(
            count("InvoiceNo").alias("times_purchased"),
            spark_sum("Quantity").alias("total_quantity_sold"),
            avg("UnitPrice").alias("avg_price"),
            spark_min("UnitPrice").alias("min_price"),
            spark_max("UnitPrice").alias("max_price"),
            spark_sum("total_price").alias("total_revenue"),
            count(col("CustomerID").isNotNull()).alias("unique_customers")
        )
        
        #add metrics
        product_catalog = product_catalog \
            .withColumn("avg_quantity_per_order",
                       col("total_quantity_sold") / col("times_purchased")) \
            .withColumn("revenue_per_customer",
                       col("total_revenue") / col("unique_customers"))
        
        #write to Silver
        silver_path = os.path.join(self.silver_path, "products")
        product_catalog.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(silver_path)
        
        print(f"✓ Created {product_catalog.count():,} products")
        print(f"✓ Written to {silver_path}")
        
        #show top products
        print("\nTop 10 products by revenue:")
        product_catalog.orderBy(col("total_revenue").desc()).show(10, truncate=False)
        
        return product_catalog
    
    def _validate_silver_transactions(self, df):
        #validate Silver transaction data
        print(f"\n Silver Transaction Validation:")
        
        total = df.count()
        returns = df.filter(col("is_return")).count()
        cancellations = df.filter(col("is_cancellation")).count()
        valid_customers = df.filter(col("CustomerID").isNotNull()).count()
        
        print(f"   Total records: {total:,}")
        print(f"   Returns: {returns:,} ({returns/total*100:.2f}%)")
        print(f"   Cancellations: {cancellations:,} ({cancellations/total*100:.2f}%)")
        print(f"   Valid customers: {valid_customers:,} ({valid_customers/total*100:.2f}%)")
        
        #date range
        date_stats = df.agg(
            spark_min("invoice_datetime").alias("min_date"),
            spark_max("invoice_datetime").alias("max_date")
        ).collect()[0]
        
        print(f"   Date range: {date_stats['min_date']} to {date_stats['max_date']}")
        
        #price statistics
        price_stats = df.filter(~col("is_return")).agg(
            spark_min("total_price").alias("min"),
            avg("total_price").alias("avg"),
            spark_max("total_price").alias("max")
        ).collect()[0]
        
        print(f"   Transaction value: ${price_stats['min']:.2f} - ${price_stats['max']:.2f} (avg: ${price_stats['avg']:.2f})")


def main():
    #Test Silver layer transformations
    from src.spark_session import create_spark_session
    
    spark = create_spark_session()
    
    try:
        silver = SilverLayer(spark)
        
        #running transformations
        silver.clean_transactions()
        silver.create_customer_profiles()
        silver.create_product_catalog()
        
        print("\n✓ Silver layer transformations completed successfully!")
        
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()