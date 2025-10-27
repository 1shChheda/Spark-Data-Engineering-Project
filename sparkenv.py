# this will be the global Spark entrypoint
import os
import sys
import yaml
from pyspark.sql import SparkSession
from dotenv import load_dotenv

def load_config(config_path="config/spark_config.yaml"):
    #To load Spark configuration from YAML file
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def init_spark(app_name=None, config_path="config/spark_config.yaml"):
    #To initialize a consistent SparkSession across the project
    #Load environment and config
    load_dotenv()
    cfg = load_config(config_path)
    spark_cfg = cfg.get("spark", {})

    #ensure Hadoop home exists (for Windows users)
    project_root = os.path.dirname(os.path.abspath(__file__))
    hadoop_path = os.path.join(project_root, "hadoop")

    if os.name == "nt":  # Only apply on Windows
        os.environ["HADOOP_HOME"] = hadoop_path
        os.environ["hadoop.home.dir"] = hadoop_path

        winutils_path = os.path.join(hadoop_path, "bin", "winutils.exe")
        if not os.path.exists(winutils_path):
            print("⚠️  Warning: winutils.exe not found at:", winutils_path)
            print("   Spark will still run, but may show permission warnings.")

    #Determine and set correct Python executable (inside venv or docker)
    python_exec = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exec
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exec

    #disable native Hadoop I/O on Windows
    os.environ["HADOOP_OPTS"] = "-Djava.library.path="
    os.environ["spark.hadoop.io.nativeio.enabled"] = "false"

    #build Spark session
    builder = (
        SparkSession.builder
        .master(spark_cfg.get("master", "local[*]"))
        .appName(app_name or spark_cfg.get("app_name", "SparkApp"))
        .config("spark.executor.memory", spark_cfg.get("executor_memory", "2g"))
        .config("spark.driver.memory", spark_cfg.get("driver_memory", "2g"))
    )

    for key, value in spark_cfg.get("additional_conf", {}).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(spark_cfg.get("log_level", "WARN"))

    print(f"✅ Spark session started: {spark.sparkContext.appName}")
    print(f"   Master: {spark.sparkContext.master}")
    print(f"   Python: {python_exec}")

    return spark
