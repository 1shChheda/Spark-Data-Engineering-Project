from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("SparkDataEngineeringApp") \
        .getOrCreate()

    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    df = spark.createDataFrame(data, ["name", "age"])
    df.show()

    df.write.mode("overwrite").csv("data/processed/sample_output")

    spark.stop()

if __name__ == "__main__":
    main()
