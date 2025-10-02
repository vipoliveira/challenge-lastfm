from typing import Dict
from pyspark.sql import SparkSession


def build_spark_session(app_name: str, spark_config: Dict[str, str]) -> SparkSession:
    
    spark_session = SparkSession.builder.appName(app_name)

    for key, value in spark_config.items():
        spark_session = spark_session.config(key, value)
    
    return spark_session.getOrCreate()