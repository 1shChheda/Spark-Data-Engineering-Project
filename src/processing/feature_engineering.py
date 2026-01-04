#Creating ML-ready features
##Builds feature store for various ML models

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    stddev, variance, datediff, current_date, when, lag, lead,
    percent_rank, ntile, expr, round as spark_round, log, sqrt,
    dayofweek, hour, month, year, collect_list, size, array_distinct
)
import os


class FeatureEngineering:
    #Handles feature engineering for ML models
    
    def __init__(self, spark: SparkSession, base_path: str = "data"):
        self.spark = spark
        self.base_path = base_path
        self.silver_path = os.path.join(base_path, "silver")
        self.gold_path = os.path.join(base_path, "gold")
        self.feature_path = os.path.join(base_path, "feature_store")
    
    def create_customer_features(self):
        #create comprehensive customer features for ML
        print("\n" + "="*60)
        print("FEATURE ENGINEERING: Customer Features")
        print("="*60)
        
        #read data
        df_trans = self.spark.read.format("delta") \
            .load(os.path.join(self.silver_path, "transactions_clean"))
        
        df_customer_360 = self.spark.read.format("delta") \
            .load(os.path.join(self.gold_path, "customer_360"))
        
        #filter valid transactions
        df_valid = df_trans.filter(
            (col("CustomerID").isNotNull()) & 
            (~col("is_return")) & 
            (~col("is_cancellation"))
        )
        
        behavior_features = df_valid.groupBy("CustomerID").agg(
            count("InvoiceNo").alias("purchase_frequency_fe"),
            avg("total_price").alias("avg_transaction_value_fe"),
            stddev("total_price").alias("std_transaction_value"),
            spark_max("total_price").alias("max_transaction_value_fe"),
            spark_min("total_price").alias("min_transaction_value_fe"),
            
            avg("Quantity").alias("avg_items_per_order_fe"),
            spark_max("Quantity").alias("max_items_per_order"),
            
            count(col("StockCode")).alias("total_products_purchased"),
            size(collect_list(col("StockCode"))).alias("unique_products_fe"),
            
            avg(when(col("is_weekend"), 1).otherwise(0)).alias("weekend_purchase_ratio_fe"),
            avg("hour").alias("avg_purchase_hour_fe"),
            stddev("hour").alias("std_purchase_hour")
        )
        
        window_customer = Window.partitionBy("CustomerID").orderBy("invoice_datetime")
        
        df_temporal = df_valid \
            .withColumn("prev_purchase_date", lag("invoice_datetime").over(window_customer)) \
            .withColumn("days_since_prev_purchase", 
                       datediff(col("invoice_datetime"), col("prev_purchase_date")))
        
        temporal_features = df_temporal.groupBy("CustomerID").agg(
            avg("days_since_prev_purchase").alias("avg_days_between_purchases_fe"),
            stddev("days_since_prev_purchase").alias("std_days_between_purchases"),
            spark_min("days_since_prev_purchase").alias("min_days_between_purchases"),
            spark_max("days_since_prev_purchase").alias("max_days_between_purchases")
        )
        
        spending_features = df_valid.groupBy("CustomerID").agg(
            spark_sum("total_price").alias("total_lifetime_value_fe"),
            (spark_sum("total_price") / count("InvoiceNo")).alias("avg_basket_value_fe"),
            
            spark_sum(when(datediff(current_date(), col("invoice_datetime")) <= 30, col("total_price"))
                     .otherwise(0)).alias("spending_last_30_days"),
            spark_sum(when(datediff(current_date(), col("invoice_datetime")) <= 90, col("total_price"))
                     .otherwise(0)).alias("spending_last_90_days"),
        )
        
        #properly handle division by zero
        spending_features = spending_features.withColumn(
            "spending_trend_30_vs_90",
            when(col("spending_last_90_days") > 0,
                 col("spending_last_30_days") / col("spending_last_90_days"))
            .otherwise(0)
        )
        
        product_affinity = df_valid.groupBy("CustomerID").agg(
            count(col("StockCode")).alias("product_variety"),
            size(array_distinct(collect_list("StockCode"))).alias("unique_stock_codes")
        )
        
        engagement = df_valid.groupBy("CustomerID").agg(
            count("InvoiceNo").alias("total_invoices_fe"),
            datediff(current_date(), spark_max("invoice_datetime")).alias("recency_days_fe"),
            datediff(spark_max("invoice_datetime"), spark_min("invoice_datetime")).alias("customer_age_days_fe")
        )
        
        #handle division by zero in engagement score
        engagement = engagement.withColumn(
            "engagement_score_fe",
            when(col("customer_age_days_fe") > 0,
                 col("total_invoices_fe") / (col("customer_age_days_fe") / 30))
            .otherwise(0)
        )
        
        #Join all features
        customer_features = df_customer_360 \
            .join(behavior_features, "CustomerID", "left") \
            .join(temporal_features, "CustomerID", "left") \
            .join(spending_features, "CustomerID", "left") \
            .join(product_affinity, "CustomerID", "left") \
            .join(engagement, "CustomerID", "left")
        
        #add derived features with proper null handling
        customer_features = customer_features \
            .withColumn("clv_to_frequency_ratio",
                       when(col("frequency") > 0, col("monetary") / col("frequency"))
                       .otherwise(0)) \
            .withColumn("recency_to_age_ratio",
                       when(col("customer_age_days_fe") > 0, 
                            col("recency_days") / col("customer_age_days_fe"))
                       .otherwise(0)) \
            .withColumn("log_monetary", log(col("monetary") + 1)) \
            .withColumn("sqrt_frequency", sqrt(col("frequency"))) \
            .withColumn("purchase_regularity",
                       when(col("std_days_between_purchases") > 0,
                            col("avg_days_between_purchases_fe") / col("std_days_between_purchases"))
                       .otherwise(0)) \
            .withColumn("spending_consistency",
                       when(col("avg_transaction_value_fe") > 0,
                            col("std_transaction_value") / col("avg_transaction_value_fe"))
                       .otherwise(0))
        
        #fill nulls
        customer_features = customer_features.fillna(0)
        
        #write to feature store
        feature_path = os.path.join(self.feature_path, "customer_features")
        customer_features.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(feature_path)
        
        print(f"✓ Created {customer_features.count():,} customer feature records")
        print(f"✓ Total features: {len(customer_features.columns)}")
        print(f"✓ Written to {feature_path}")
        
        print("\n Sample Features:")
        customer_features.select(
            "CustomerID", "recency_days", "frequency", "monetary",
            "avg_transaction_value", "unique_products", "engagement_score_fe",
            "customer_segment", "value_tier"
        ).show(5, truncate=False)
        
        return customer_features
    
    def create_product_features(self):
        #create product features for recommendation systems
        print("\n" + "="*60)
        print("FEATURE ENGINEERING: Product Features")
        print("="*60)
        
        df_trans = self.spark.read.format("delta") \
            .load(os.path.join(self.silver_path, "transactions_clean"))
        
        df_products = self.spark.read.format("delta") \
            .load(os.path.join(self.gold_path, "product_metrics"))
        
        #print(f"Product metrics columns: {df_products.columns}")
        
        df_valid = df_trans.filter(
            (~col("is_return")) & 
            (~col("is_cancellation"))
        )
        
        popularity = df_valid.groupBy("StockCode").agg(
            count("InvoiceNo").alias("purchase_count_fe"),
            count(col("CustomerID").isNotNull()).alias("unique_buyers_fe"),
            spark_sum("Quantity").alias("total_quantity_sold_fe"),
            avg("UnitPrice").alias("avg_price_fe"),
            stddev("UnitPrice").alias("price_volatility")
        )
        
        copurchase = df_valid.groupBy("InvoiceNo").agg(
            collect_list("StockCode").alias("products_in_basket")
        )
        
        product_features = df_products.join(popularity, "StockCode", "left")
        
        #add proper null handling in derived features
        product_features = product_features \
            .withColumn("popularity_score",
                       when(col("unique_buyers") > 0,
                            log(col("unique_buyers") + 1))
                       .otherwise(0)) \
            .withColumn("price_tier",
                       when(col("avg_transaction_value") >= 10, "Premium")
                       .when(col("avg_transaction_value") >= 5, "Mid-Range")
                       .otherwise("Budget")) \
            .withColumn("velocity_score",
                       when(col("purchase_frequency") > 0,
                            col("total_revenue") / col("purchase_frequency"))
                       .otherwise(0)) \
            .withColumn("customer_concentration",
                       when(col("purchase_frequency") > 0,
                            col("unique_buyers") / col("purchase_frequency"))
                       .otherwise(0))
        
        product_features = product_features.fillna(0)
        
        feature_path = os.path.join(self.feature_path, "product_features")
        product_features.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(feature_path)
        
        print(f"✓ Created {product_features.count():,} product feature records")
        print(f"✓ Written to {feature_path}")
        
        print("\n Sample Product Features:")
        product_features.select(
            "StockCode", "Description", "total_revenue", 
            "unique_buyers", "popularity_score", "price_tier"
        ).orderBy(col("total_revenue").desc()).show(5, truncate=False)
        
        return product_features
    
    def create_transaction_features(self):
        #create transaction-level features for fraud detection
        print("\n" + "="*60)
        print("FEATURE ENGINEERING: Transaction Features")
        print("="*60)
        
        df_trans = self.spark.read.format("delta") \
            .load(os.path.join(self.silver_path, "transactions_clean"))
        
        customer_stats = df_trans.groupBy("CustomerID").agg(
            avg("total_price").alias("customer_avg_price"),
            stddev("total_price").alias("customer_std_price"),
            avg("Quantity").alias("customer_avg_quantity"),
            stddev("Quantity").alias("customer_std_quantity")
        )
        
        trans_features = df_trans.join(customer_stats, "CustomerID", "left")
        
        #add proper null handling for z-scores
        trans_features = trans_features \
            .withColumn("price_zscore",
                       when(col("customer_std_price") > 0,
                            (col("total_price") - col("customer_avg_price")) / col("customer_std_price"))
                       .otherwise(0)) \
            .withColumn("quantity_zscore",
                       when(col("customer_std_quantity") > 0,
                            (col("Quantity") - col("customer_avg_quantity")) / col("customer_std_quantity"))
                       .otherwise(0))
        
        trans_features = trans_features \
            .withColumn("is_business_hours", 
                       when((col("hour") >= 9) & (col("hour") <= 17), 1).otherwise(0)) \
            .withColumn("is_night_purchase",
                       when((col("hour") >= 22) | (col("hour") <= 6), 1).otherwise(0))
        
        feature_path = os.path.join(self.feature_path, "transaction_features")
        trans_features.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("year", "month") \
            .save(feature_path)
        
        print(f"✓ Created {trans_features.count():,} transaction feature records")
        print(f"✓ Written to {feature_path}")
        
        return trans_features


def main():
    from src.spark_session import create_spark_session
    
    spark = create_spark_session()
    
    try:
        fe = FeatureEngineering(spark)
        
        fe.create_customer_features()
        fe.create_product_features()
        fe.create_transaction_features()
        
        print("\n✓ Feature engineering completed successfully!")
        
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()