#Raw Data Ingestion Layer
##Reads Excel file and writes to Delta Lake without transformation

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, lit, col, sha2, concat_ws
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType
)
from delta import DeltaTable
import os


class BronzeLayer:
    def __init__(self, spark: SparkSession, base_path: str = "data"):
        self.spark = spark
        self.base_path = base_path
        self.bronze_path = os.path.join(base_path, "bronze")

    def define_schema(self):
        return StructType([
            StructField("InvoiceNo", StringType(), True),
            StructField("StockCode", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("Quantity", IntegerType(), True),
            StructField("InvoiceDate", StringType(), True),
            StructField("UnitPrice", DoubleType(), True),
            StructField("CustomerID", StringType(), True),
            StructField("Country", StringType(), True)
        ])

    def read_excel(self, file_path: str):
        print(f"Reading Excel file: {file_path}")
        import pandas as pd

        df_pandas = pd.read_excel(file_path, engine="openpyxl")
        df = self.spark.createDataFrame(df_pandas)

        print(f"✓ Read {df.count()} rows from Excel")
        return df

    def write_to_bronze(self, df, table_name: str):
        table_path = os.path.join(self.bronze_path, table_name)

        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(table_path)

        return table_path

    def ingest_transactions(self, excel_path: str):
        df = self.read_excel(excel_path)

        df_with_metadata = df

        table_path = self.write_to_bronze(df_with_metadata, "transactions")
        return table_path


def main():
    from src.spark_session import create_spark_session
    spark = create_spark_session()

    bronze = BronzeLayer(spark)
    bronze.ingest_transactions("data/raw/Online_Retail.xlsx")
    spark.stop()


if __name__ == "__main__":
    main()