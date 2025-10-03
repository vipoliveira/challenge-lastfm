import os

from src.constants import SessionConfig
from src.base import BasePipeline
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

class ResultsPipeline(BasePipeline):

    def read(self, **kwargs) -> dict:
        events_path = kwargs.get("events_path")
        profiles_path = kwargs.get("profiles_path")
        
        if not events_path or not profiles_path:
            raise ValueError("Both events_path and profiles_path must be provided")
        
        events_df = (
            self.spark.read.option("header", True)
                .option("sep", "\t")
                .csv(events_path)
        )
        
        profiles_df = (
            self.spark.read.option("header", True)
                .option("sep", "\t")
                .csv(profiles_path)
        )
        
        return {"events_df": events_df, "profiles_df": profiles_df}

    def transform(self, data_dict: dict) -> DataFrame:
        events_df = data_dict["events_df"]
        profiles_df = data_dict["profiles_df"]
        
        events_with_profiles = events_df.join(profiles_df, on="user_id", how="left")
        
        top_songs = (
            events_with_profiles.groupBy("track_id", "track_name")
            .agg(
                F.count("*").alias("plays"),
                F.countDistinct("user_id").alias("distinct_users"),
                F.array_sort(F.collect_set("country")).alias("countries_arr"),
            )
            .withColumn("countries", F.concat_ws(",", F.col("countries_arr")))
            .drop("countries_arr")
            .orderBy(F.col("plays").desc(), F.col("track_name"))
            .limit(SessionConfig.TOP_SONGS_COUNT.value)
        )
        
        return top_songs
        
    def write(self, df: DataFrame, output_path: str):
        (
            df.coalesce(1)
            .select(
                F.col("track_id").alias("track_id"),
                F.col("track_name").alias("track_name"),
                F.col("plays").alias("plays"),
                F.col("distinct_users").alias("distinct_users"),
                F.col("countries").alias("countries"),
            )
            .write.mode("overwrite")
            .option("header", True)
            .option("sep", "\t")
            .csv(output_path)
        )
        
        tsv_path = os.path.join(os.path.dirname(output_path), "top_songs.tsv")
        df.coalesce(1).write.mode("overwrite").option("header", True).option("sep", "\t").csv(tsv_path + ".dir")
    
    def validate(self, data) -> bool:
        if isinstance(data, dict):
            events_df = data.get("events_df")
            profiles_df = data.get("profiles_df")
            
            if events_df is None or profiles_df is None:
                raise ValueError("Both events_df and profiles_df must be provided for validation")
            
            events_gdf = SparkDFDataset(events_df)
            events_gdf.expect_column_values_to_not_be_null("user_id")
            events_gdf.expect_column_values_to_not_be_null("track_id")
            events_gdf.expect_column_values_to_not_be_null("track_name")
            events_gdf.expect_column_values_to_not_be_null("session_id")
            
            events_results = events_gdf.validate(result_format="SUMMARY")
            if not events_results.get("success", False):
                raise RuntimeError(f"Events validation failed: {events_results}")
            
            profiles_gdf = SparkDFDataset(profiles_df)
            profiles_gdf.expect_column_values_to_not_be_null("user_id")
            profiles_gdf.expect_column_values_to_not_be_null("country")
            
            profiles_results = profiles_gdf.validate(result_format="SUMMARY")
            if not profiles_results.get("success", False):
                raise RuntimeError(f"Profiles validation failed: {profiles_results}")
            
            print("Input data validation completed successfully.")
            return True
        
        df = data
        gdf = SparkDFDataset(df)
        
        gdf.expect_column_values_to_not_be_null("track_id")
        gdf.expect_column_values_to_not_be_null("track_name")
        gdf.expect_column_values_to_not_be_null("plays")
        gdf.expect_column_values_to_not_be_null("distinct_users")
        
        gdf.expect_column_values_to_be_of_type("plays", "IntegerType")
        gdf.expect_column_values_to_be_of_type("distinct_users", "IntegerType")
        
        gdf.expect_column_values_to_be_between("plays", min_value=1)
        gdf.expect_column_values_to_be_between("distinct_users", min_value=1)
        
        gdf.expect_column_value_lengths_to_be_between("track_id", min_value=1)
        gdf.expect_column_value_lengths_to_be_between("track_name", min_value=1, max_value=500)
        
        count = df.count()
        if count != SessionConfig.TOP_SONGS_COUNT.value:
            raise RuntimeError(f"Expected {SessionConfig.TOP_SONGS_COUNT.value} results, got {count}")
        
        results = gdf.validate(result_format="SUMMARY")
        if not results.get("success", False):
            raise RuntimeError(f"Results validation failed: {results}")
            
        print(f"Results validation completed successfully. Found {count} top songs.")
        return True

class ProfilesPipeline(BasePipeline):

    DATASET_DIR = "lastfm-dataset-1k"
    PROFILES_FILENAME = "userid-profile.tsv"


    def read(self, input_path: str) -> DataFrame:
        file_path = os.path.join(input_path, self.DATASET_DIR, self.PROFILES_FILENAME)
        df = (
            self.spark.read.option("header", True)
                .option("sep", "\t")
                .csv(file_path)
        )
        
        return df
    
    def transform(self, df: DataFrame) -> DataFrame:
        return df.select(
            F.col("userid").alias("user_id"),
            F.col("country").alias("country"),
        )

    def write(self, df: DataFrame, output_path: str):
        (
            df.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .option("sep", "\t")
            .csv(output_path)
        )
    
    def validate(self, df: DataFrame) -> bool:
        gdf = SparkDFDataset(df)
        
        # Expectations
        gdf.expect_column_values_to_not_be_null("user_id")
        gdf.expect_column_values_to_not_be_null("country")
        
        results = gdf.validate(result_format="SUMMARY")
        if not results.get("success", False):
            raise RuntimeError(f"Great Expectations validation failed: {results}")
            
        return True
    

class EventsPipeline(BasePipeline):

    DATASET_DIR = "lastfm-dataset-1k"
    EVENTS_FILENAME = "userid-timestamp-artid-artname-traid-traname.tsv"

    def read(self, input_path: str) -> DataFrame:
        file_path = os.path.join(input_path, self.DATASET_DIR, self.EVENTS_FILENAME)
        df = (
            self.spark.read.option("header", True)
                .option("sep", "\t")
                .csv(file_path)
        )
        
        return df
    
    def _transformation_1(self, df: DataFrame) -> DataFrame:
        return df.select(
            F.col("userid").alias("user_id"),
            F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("start_time"),
            F.col("traid").alias("track_id"),
            F.col("traname").alias("track_name")
        ).filter(F.col("track_id").isNotNull())
    
    def _transformation_2(self, df: DataFrame) -> DataFrame:
        user_time_window = Window.partitionBy("user_id").orderBy("start_time")
        
        return df.withColumn("prev_start", F.lag("start_time").over(user_time_window))\
            .withColumn(
                "is_new_session",
                F.when(
                    (F.col("prev_start").isNull()) | 
                    (F.col("start_time").cast("long") - F.col("prev_start").cast("long") > 
                    SessionConfig.SESSION_GAP_MINUTES.value * 60), 
                    1
                ).otherwise(0)
            )\
            .withColumn(
                "session_seq",
                F.sum("is_new_session").over(user_time_window.rowsBetween(Window.unboundedPreceding, 0))
            )\
            .withColumn("session_id", F.concat_ws(":", F.col("user_id"), F.col("session_seq")))\
            .drop("prev_start", "is_new_session", "session_seq")
    
    def _transformation_3(self, df: DataFrame) -> DataFrame:
        session_track_counts = df.groupBy("session_id")\
            .agg(F.countDistinct("track_id").alias("tracks_count"))
            
        window_top = Window.orderBy(F.col("tracks_count").desc(), F.col("session_id"))
        return session_track_counts.withColumn(
                "rank", 
                F.row_number().over(window_top)
            )\
            .filter(F.col("rank") <= SessionConfig.TOP_SESSIONS_COUNT.value)\
            .drop("rank")
    
    def _transformation_4(self, sessions_df: DataFrame, top_sessions: DataFrame) -> DataFrame:
        return sessions_df.join(top_sessions, on="session_id", how="inner")
    
    def transform(self, df: DataFrame) -> DataFrame:
        
        events_df = self._transformation_1(df)
    
        sessions_df = self._transformation_2(events_df)
        
        top_sessions = self._transformation_3(sessions_df)
        
        return self._transformation_4(sessions_df, top_sessions)

    def write(self, df: DataFrame, output_path: str):
        (
            df.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .option("sep", "\t")
            .csv(output_path)
        )
    
    def validate(self, df: DataFrame) -> bool:
        gdf = SparkDFDataset(df)
        gdf.expect_column_values_to_not_be_null("user_id")
        gdf.expect_column_values_to_not_be_null("start_time")
        gdf.expect_column_values_to_not_be_null("track_id")
        gdf.expect_column_values_to_not_be_null("track_name")

        gdf.expect_column_values_to_match_regex("track_id", r"^.{1,}$")
        gdf.expect_column_values_to_be_of_type("start_time", "TimestampType")

        gdf.expect_column_values_to_be_between(
            "start_time", min_value="2000-01-01 00:00:00", max_value="2030-01-01 00:00:00"
        )

        gdf.expect_column_value_lengths_to_be_between("track_name", min_value=1, max_value=500)
        
        if "session_id" in df.columns:
            gdf.expect_column_values_to_not_be_null("session_id")
            
            # Check that we have top sessions if this is the filtered dataset
            if "tracks_count" in df.columns:
                session_count = df.select("session_id").distinct().count()
                if session_count > SessionConfig.TOP_SESSIONS_COUNT.value:
                    print(f"Warning: Found {session_count} sessions, expected {SessionConfig.TOP_SESSIONS_COUNT.value} or fewer")

        results = gdf.validate(result_format="SUMMARY")
        if not results.get("success", False):
            raise RuntimeError(f"Great Expectations validation failed: {results}")
            
        return True