#Business-Ready Analytics Tables Layer
##Creates aggregated, analytics-ready datasets

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, sum as spark_sum, avg, count, max as spark_max, 
    min as spark_min, datediff, current_date, dense_rank, 
    percent_rank, ntile, expr, round as spark_round, concat_ws,
    year, month, quarter, to_date, date_trunc
)
import os


class GoldLayer:
    
    def __init__(self, spark: SparkSession, base_path: str = "data"):
        self.spark = spark
        self.base_path = base_path
        self.silver_path = os.path.join(base_path, "silver")
        self.gold_path = os.path.join(base_path, "gold")
        
    def create_customer_360(self):
        print("\n" + "="*60)
        print("GOLD LAYER: Creating Customer 360 View")
        print("="*60)
        
        #read Silver tables
        df_trans = self.spark.read.format("delta") \
            .load(os.path.join(self.silver_path, "transactions_clean"))
        
        #added missing customers table read
        df_customers = self.spark.read.format("delta") \
            .load(os.path.join(self.silver_path, "customers"))
        
        #filter valid transactions
        df_valid = df_trans.filter(
            (col("CustomerID").isNotNull()) & 
            (~col("is_return")) & 
            (~col("is_cancellation"))
        )
        
        #calculate RFM metrics
        current_date_val = df_valid.agg(spark_max("invoice_datetime")).collect()[0][0]
        
        rfm = df_valid.groupBy("CustomerID").agg(
            datediff(
                expr(f"to_date('{current_date_val}')"),
                spark_max("invoice_datetime")
            ).alias("recency_days"),
            count("InvoiceNo").alias("frequency"),
            spark_sum("total_price").alias("monetary")
        )
        
        window_spec = Window.orderBy(col("recency_days"))
        rfm_scored = rfm \
            .withColumn("r_score", 6 - ntile(5).over(window_spec)) \
            .withColumn("f_score", ntile(5).over(Window.orderBy(col("frequency")))) \
            .withColumn("m_score", ntile(5).over(Window.orderBy(col("monetary")))) \
            .withColumn("rfm_score", 
                       concat_ws("", col("r_score"), col("f_score"), col("m_score")))
        
        #segment customers
        rfm_segmented = rfm_scored.withColumn(
            "customer_segment",
            when((col("r_score") >= 4) & (col("f_score") >= 4), "Champions")
            .when((col("r_score") >= 3) & (col("f_score") >= 3), "Loyal Customers")
            .when((col("r_score") >= 4) & (col("f_score") <= 2), "Promising")
            .when((col("r_score") <= 2) & (col("f_score") >= 4), "At Risk")
            .when((col("r_score") <= 2) & (col("f_score") <= 2), "Hibernating")
            .otherwise("Potential Loyalists")
        )
        
        #additional behavioral metrics
        behavior_metrics = df_valid.groupBy("CustomerID").agg(
            avg("total_price").alias("avg_order_value"),
            spark_max("total_price").alias("max_order_value"),
            spark_min("total_price").alias("min_order_value"),
            count(col("InvoiceNo").isNotNull()).alias("unique_invoices"),
            (spark_sum("total_price") / count(col("InvoiceNo").isNotNull())).alias("avg_basket_value"),
            avg("Quantity").alias("avg_items_per_transaction")
        )
        
        #added product diversity metrics
        product_diversity = df_valid.groupBy("CustomerID").agg(
            count(col("StockCode")).alias("unique_products"),
            count(col("Description")).alias("unique_categories")
        )
        
        #added temporal patterns
        temporal = df_valid.groupBy("CustomerID").agg(
            avg(when(col("is_weekend"), 1).otherwise(0)).alias("weekend_purchase_ratio"),
            avg("hour").alias("avg_purchase_hour")
        )
        
        #join all metrics
        customer_360 = df_customers \
            .join(rfm_segmented, "CustomerID", "left") \
            .join(behavior_metrics, "CustomerID", "left") \
            .join(product_diversity, "CustomerID", "left") \
            .join(temporal, "CustomerID", "left")
        
        #write to Gold
        gold_path = os.path.join(self.gold_path, "customer_360")
        customer_360.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(gold_path)
        
        print(f"✓ Created Customer 360 with {customer_360.count():,} customers")
        
        return customer_360
    
    def create_product_metrics(self):
        print("\n" + "="*60)
        print("GOLD LAYER: Creating Product Metrics")
        print("="*60)
        
        df_trans = self.spark.read.format("delta") \
            .load(os.path.join(self.silver_path, "transactions_clean"))
        
        #added missing products table read
        df_products = self.spark.read.format("delta") \
            .load(os.path.join(self.silver_path, "products"))
        
        #valid transactions only
        df_valid = df_trans.filter(
            (~col("is_return")) & 
            (~col("is_cancellation"))
        )
        
        #product performance metrics
        product_perf = df_valid.groupBy("StockCode").agg(
            spark_sum("total_price").alias("total_revenue_gold"),
            spark_sum("Quantity").alias("total_quantity_gold"),
            count(col("CustomerID").isNotNull()).alias("unique_buyers"),
            avg("total_price").alias("avg_transaction_value"),
            count("InvoiceNo").alias("purchase_frequency")
        )
        
        #drop duplicate columns from df_products before join
        product_gold = df_products \
            .drop("total_revenue", "total_quantity_sold") \
            .join(product_perf, "StockCode", "left") \
            .withColumnRenamed("total_revenue_gold", "total_revenue") \
            .withColumnRenamed("total_quantity_gold", "total_quantity_sold")
        
        #add performance categories
        product_gold = product_gold.withColumn(
            "performance_category",
            when(col("total_revenue") >= product_gold.approxQuantile("total_revenue", [0.8], 0.01)[0], "Star Products")
            .when(col("total_revenue") >= product_gold.approxQuantile("total_revenue", [0.5], 0.01)[0], "Good Performers")
            .when(col("total_revenue") >= product_gold.approxQuantile("total_revenue", [0.2], 0.01)[0], "Average")
            .otherwise("Underperformers")
        )
        
        #write to Gold
        gold_path = os.path.join(self.gold_path, "product_metrics")
        product_gold.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(gold_path)
        
        print(f"✓ Created product metrics for {product_gold.count():,} products")
        
        #show top products
        print("\n Top 10 Products by Revenue:")
        product_gold.select(
            "StockCode", "Description", "total_revenue", 
            "total_quantity_sold", "unique_buyers", "performance_category"
        ).orderBy(col("total_revenue").desc()).show(10, truncate=False)
        
        return product_gold


def main():
    from src.spark_session import create_spark_session
    
    spark = create_spark_session()
    
    try:
        gold = GoldLayer(spark)
        
        gold.create_customer_360()
        gold.create_product_metrics()
        
        print("\n✓ Gold layer analytics completed successfully!")
        
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()