from pyspark.sql import SparkSession
import yaml
import os

def create_spark_session():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'spark_config.yaml')
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    spark_config = config['spark']
    builder = SparkSession.builder.appName(spark_config['app_name'])
    
    if 'master' in spark_config:
        builder = builder.master(spark_config['master'])
    
    for key, value in spark_config.get('config', {}).items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def stop_spark_session(spark):
    if spark:
        spark.stop()