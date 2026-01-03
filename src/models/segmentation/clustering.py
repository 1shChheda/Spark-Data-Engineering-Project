from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col
import os


class CustomerSegmentation:

    def __init__(self, spark, base_path="data"):
        self.spark = spark
        self.feature_path = os.path.join(base_path, "feature_store")

    def load_features(self):
        return self.spark.read.format("delta") \
            .load(os.path.join(self.feature_path, "customer_features"))

    def prepare_features(self, df):
        feature_cols = [
            "recency_days",
            "frequency",
            "monetary",
            "customer_segment"
        ]
        return df, feature_cols

    def run(self):
        df = self.load_features()
        df, features = self.prepare_features(df)

        assembler = VectorAssembler(
            inputCols=features,
            outputCol="features"
        )

        assembled = assembler.transform(df)

        model = KMeans(k=5).fit(assembled)
        return model
