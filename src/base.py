from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, DataFrame


class BasePipeline(ABC):
    
    def __init__(self, spark: SparkSession):
        self.spark = spark

    @abstractmethod
    def read(self, input_path: str) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def write(self, df: DataFrame, output_path: str):
        raise NotImplementedError

    @abstractmethod
    def validate(self, df: DataFrame) -> bool:
        raise NotImplementedError