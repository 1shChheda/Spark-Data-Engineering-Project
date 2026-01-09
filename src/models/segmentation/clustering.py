from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, when
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
                "recency_days",
                "frequency",
                "monetary",
                "avg_transaction_value",
                "unique_products",
                "customer_age_days_fe",
                "engagement_score_fe",
                "spending_last_30_days",
                "spending_last_90_days"
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
    
    def find_optimal_k(self, df, feature_cols, k_range=(2, 11)):
        #find optimal number of clusters using elbow method
        print("\nFinding optimal number of clusters...")
        
        #assemble features
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features_raw"
        )
        
        #scale features
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withMean=True,
            withStd=True
        )
        
        #transform data
        df_assembled = assembler.transform(df)
        scaler_model = scaler.fit(df_assembled)
        df_scaled = scaler_model.transform(df_assembled)
        
        #cache for multiple iterations
        df_scaled.cache()
        
        #try different k values
        costs = []
        for k in range(k_range[0], k_range[1]):
            print(f"  Testing k={k}...")
            
            kmeans = KMeans(
                k=k,
                seed=42,
                maxIter=20,
                featuresCol="features",
                predictionCol="cluster"
            )
            
            model = kmeans.fit(df_scaled)
            cost = model.summary.trainingCost
            costs.append((k, cost))
            
            print(f"    Cost: {cost:.2f}")
        
        #unpersist to free memory
        df_scaled.unpersist()
        
        #find elbow point (simple heuristic)
        optimal_k = 5  # Default
        
        print(f"\n✓ Recommended k={optimal_k} (we can adjust based on business needs)")
        
        return optimal_k, costs
    
    def train_clustering_model(self, df, feature_cols, n_clusters=5):
        #train K-Means clustering model
        print(f"\nTraining K-Means model with k={n_clusters}...")
        
        #build pipeline
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features_raw"
        )
        
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withMean=True,
            withStd=True
        )
        
        kmeans = KMeans(
            k=n_clusters,
            seed=42,
            maxIter=100,
            featuresCol="features",
            predictionCol="cluster_id"
        )
        
        pipeline = Pipeline(stages=[assembler, scaler, kmeans])
        
        #train model
        model = pipeline.fit(df)
        
        print(f"✓ Model trained successfully")
        
        #get cluster centers (for interpretation)
        kmeans_model = model.stages[-1]
        print(f"✓ Cluster centers computed")
        
        return model, kmeans_model
    
    def assign_segments(self, df, model):
        #assign clusters and create business segments
        print("\nAssigning segments to customers...")
        
        #predict clusters
        df_clustered = model.transform(df)
        
        #calculate cluster statistics for naming
        cluster_stats = df_clustered.groupBy("cluster_id").agg(
            {"recency_days": "avg", "frequency": "avg", "monetary": "avg"}
        ).collect()
        
        #create mapping based on RFM characteristics
        segment_mapping = {}
        for row in cluster_stats:
            cluster_id = row["cluster_id"]
            avg_recency = row["avg(recency_days)"]
            avg_frequency = row["avg(frequency)"]
            avg_monetary = row["avg(monetary)"]
            
            #simple heuristic for segment naming
            if avg_recency < 60 and avg_frequency > 5 and avg_monetary > 500:
                segment_mapping[cluster_id] = "Champions"
            elif avg_recency < 90 and avg_frequency > 3:
                segment_mapping[cluster_id] = "Loyal Customers"
            elif avg_recency < 60 and avg_frequency <= 3:
                segment_mapping[cluster_id] = "Promising"
            elif avg_recency >= 180 and avg_frequency > 3:
                segment_mapping[cluster_id] = "At Risk"
            elif avg_recency >= 180:
                segment_mapping[cluster_id] = "Hibernating"
            else:
                segment_mapping[cluster_id] = "Needs Attention"
        
        #apply mapping
        mapping_expr = when(col("cluster_id") == 0, segment_mapping.get(0, "Other"))
        for cluster_id in range(1, len(segment_mapping)):
            mapping_expr = mapping_expr.when(
                col("cluster_id") == cluster_id, 
                segment_mapping.get(cluster_id, "Other")
            )
        
        df_segmented = df_clustered.withColumn("segment_name", mapping_expr)
        
        print(f"✓ Assigned segments to {df_segmented.count():,} customers")
        
        return df_segmented
    
    def generate_segment_profiles(self, df_segmented):
        #generate detailed segment profiles
        print("\nGenerating segment profiles...")
        
        segment_profiles = df_segmented.groupBy("segment_name").agg(
            {"CustomerID": "count",
             "recency_days": "avg",
             "frequency": "avg",
             "monetary": "avg",
             "avg_transaction_value": "avg",
             "unique_products": "avg",
             "engagement_score_fe": "avg"}  # use _fe version
        ).orderBy(col("count(CustomerID)").desc())
        
        print("\n Segment Profiles:")
        segment_profiles.show(truncate=False)
        
        return segment_profiles
    
    def save_model(self, model, model_metadata):
        # TODO: save model and metadata
        pass
    
    def save_segments(self, df_segmented):
        # TODO: save segmented customers
        pass
    
    def run_segmentation(self, n_clusters=5):
        #segmentation workflow (partial)
        print("\n" + "="*60)
        print("CUSTOMER SEGMENTATION PIPELINE")
        print("="*60)
        
        #load features
        df = self.load_features()
        
        #prepare features
        df_prepared, feature_cols = self.prepare_features(df)
        
        #find optimal k
        optimal_k, costs = self.find_optimal_k(df_prepared, feature_cols)
        #fix: use optimal_k
        n_clusters = optimal_k
        
        #train model
        model, kmeans_model = self.train_clustering_model(
            df_prepared, 
            feature_cols, 
            n_clusters=n_clusters
        )
        
        #assign segments
        df_segmented = self.assign_segments(df_prepared, model)
        
        #generate profiles
        segment_profiles = self.generate_segment_profiles(df_segmented)
        
        # TODO: saves...
        print("\n✓ Partial pipeline executed!")


def main():
    # TODO: test customer segmentation
    pass


if __name__ == "__main__":
    main()