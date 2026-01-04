from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os


class CustomerSegmentation:

    def __init__(self, spark: SparkSession, base_path: str = "data"):
        self.spark = spark
        self.base_path = base_path
        self.feature_path = os.path.join(base_path, "feature_store")
        self.model_path = os.path.join(base_path, "models", "segmentation")
        self.gold_path = os.path.join(base_path, "gold")
    
    def load_features(self):
        # load customer features
        print("Loading customer features...")
        
        df = self.spark.read.format("delta") \
            .load(os.path.join(self.feature_path, "customer_features"))
        
        print(f"✓ Loaded {df.count():,} customer records")
        return df
    
    def prepare_features(self, df, feature_cols=None):
        # prepare features for clustering
        print("\nPreparing features for clustering...")
        
        if feature_cols is None:
            # select key features for segmentation
            feature_cols = [
                "recency_days",           # fixed
                "frequency",              # from customer_360
                "monetary",               # fixed
                "avg_transaction_value",  # from customer_360
                "unique_products",        # from customer_360
                "customer_age_days_fe",   # use _fe version
                "engagement_score_fe",    # use _fe version
                "spending_last_30_days",  # from feature engineering
                "spending_last_90_days"   # from feature engineering
            ]
        
        # filter out customers with insufficient data
        df_filtered = df.filter(
            (col("frequency") > 0) & 
            (col("monetary") > 0)
        )
        
        #fill nulls
        for col_name in feature_cols:
            df_filtered = df_filtered.fillna(0, [col_name])
        
        print(f"✓ Using {len(feature_cols)} features for clustering")
        print(f"✓ Filtered dataset: {df_filtered.count():,} customers")
        
        return df_filtered, feature_cols

def main():
    # TODO: test customer segmentation
    pass


if __name__ == "__main__":
    main()