#Raw Data Ingestion Layer
##Reads Excel file and writes to Delta Lake without transformation

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, lit, col, sha2, concat_ws,
    year, month, dayofmonth, hour, minute
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType
)
from delta import DeltaTable
import os
from datetime import datetime


class BronzeLayer:
    #Handles raw data ingestion into Bronze layer
    
    def __init__(self, spark: SparkSession, base_path: str = "data"):
        self.spark = spark
        self.base_path = base_path
        self.bronze_path = os.path.join(base_path, "bronze")
        
    def define_schema(self):
        #define explicit schema for Online Retail data
        return StructType([
            StructField("InvoiceNo", StringType(), True),
            StructField("StockCode", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("Quantity", IntegerType(), True),
            StructField("InvoiceDate", StringType(), True),  # Will parse later
            StructField("UnitPrice", DoubleType(), True),
            StructField("CustomerID", StringType(), True),  # String to handle nulls
            StructField("Country", StringType(), True)
        ])
    
    def read_excel(self, file_path: str):
        #read Excel file with proper schema
        print(f"Reading Excel file: {file_path}")
        
        #read using pandas first (pyspark doesn't natively support Excel well)
        import pandas as pd
        
        df_pandas = pd.read_excel(file_path, engine='openpyxl')
        
        #convert to spark dataframe with schema
        df = self.spark.createDataFrame(df_pandas)
        
        print(f"✓ Read {df.count()} rows from Excel")
        return df
    
    def add_metadata(self, df):
        #add metadata columns for lineage and auditing
        return df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", lit("Online_Retail.xlsx")) \
            .withColumn("bronze_layer_version", lit("v1.0")) \
            .withColumn("record_hash", 
                       sha2(concat_ws("|", 
                                     col("InvoiceNo"),
                                     col("StockCode"),
                                     col("InvoiceDate")), 256))
    
    def write_to_bronze(self, df, table_name: str):
        #write dataframe to Bronze Delta table
        table_path = os.path.join(self.bronze_path, table_name)
        
        print(f"Writing to Bronze layer: {table_path}")
        
        #write as Delta table
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(table_path)
        
        print(f"✓ Written to {table_path}")
        
        #show statistics
        delta_table = DeltaTable.forPath(self.spark, table_path)
        print(f"✓ Delta table created with {df.count()} rows")
        
        return table_path
    
    def ingest_transactions(self, excel_path: str):
        #Main method: Read Excel and ingest to Bronze
        print("\n" + "="*60)
        print("BRONZE LAYER: Ingesting Raw Transactions")
        print("="*60)
        
        #read excel
        df = self.read_excel(excel_path)
        
        #add metadata
        df_with_metadata = self.add_metadata(df)
        
        #show sample
        print("\nSample data:")
        df_with_metadata.select(
            "InvoiceNo", "StockCode", "Description", 
            "Quantity", "UnitPrice", "Country",
            "ingestion_timestamp", "record_hash"
        ).show(5, truncate=False)
        
        #write to Bronze
        table_path = self.write_to_bronze(df_with_metadata, "transactions")
        
        #validation
        self._validate_bronze_table(table_path)
        
        return table_path
    
    def _validate_bronze_table(self, table_path: str):
        #validate Bronze table after ingestion
        df = self.spark.read.format("delta").load(table_path)
        
        total_rows = df.count()
        null_customers = df.filter(col("CustomerID").isNull()).count()
        negative_quantities = df.filter(col("Quantity") < 0).count()
        
        print(f"\n Bronze Table Statistics:")
        print(f"   Total rows: {total_rows:,}")
        print(f"   Null customers: {null_customers:,} ({null_customers/total_rows*100:.2f}%)")
        print(f"   Negative quantities (returns): {negative_quantities:,}")
        
        #show schema
        print(f"\n Schema:")
        df.printSchema()
        
    def read_bronze_table(self, table_name: str):
        #read Bronze Delta table
        table_path = os.path.join(self.bronze_path, table_name)
        return self.spark.read.format("delta").load(table_path)


def main():
    #Test Bronze layer ingestion
    from src.spark_session import create_spark_session
    
    spark = create_spark_session()
    
    try:
        bronze = BronzeLayer(spark)
        
        #ingest data
        excel_file = "data/raw/Online_Retail.xlsx"
        bronze.ingest_transactions(excel_file)
        
        print("\n✓ Bronze layer ingestion completed successfully!")
        
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()