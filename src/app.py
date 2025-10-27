from src.spark_session import create_spark_session, stop_spark_session
import os

def main():
    print("Starting Data Engineering Application...")
    
    spark = create_spark_session()
    
    try:
        data = [
            (1, "Alice", 29, "Engineer"),
            (2, "Bob", 35, "Data Scientist"),
            (3, "Charlie", 42, "Manager")
        ]
        
        columns = ["id", "name", "age", "role"]
        df = spark.createDataFrame(data, columns)
        
        print("\n=== DataFrame Created ===")
        df.show()
        
        output_path = "data/processed/output.csv"
        os.makedirs("data/processed", exist_ok=True)
        
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        
        print(f"\n=== Data written to {output_path} ===")
        
        read_df = spark.read.option("header", "true").csv(output_path)
        print("\n=== Data read back from CSV ===")
        read_df.show()
        
        print("\n✓ Application completed successfully!")
        
    except Exception as e:
        print(f"✗ Error occurred: {str(e)}")
        raise
    finally:
        stop_spark_session(spark)

if __name__ == "__main__":
    main()