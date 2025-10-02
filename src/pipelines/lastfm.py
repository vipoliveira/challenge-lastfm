import os

from typing import Optional
from src.factory import build_spark_session
from src.constants import SparkConfig, AirflowConfig, LastFmFiles, SessionConfig
from src.base import BasePipeline
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from great_expectations.dataset.sparkdf_dataset import SparkDFDataset


class Entrypoint:

    def __init__(self, spark: Optional[SparkSession] = None) -> None:
        self.spark = spark or build_spark_session()

    def run(self, input_dir: str = None, output_dir: str = None, validate_only: bool = False):
        """Main pipeline execution."""
        
        input_dir = input_dir or AirflowConfig.DEFAULT_LASTFM_DIR.value
        output_dir = output_dir or AirflowConfig.DEFAULT_OUTPUT_DIR.value
        
        # Read and transform events data (includes all processing)
        events_pipeline = EventsPipeline(spark=self.spark)
        events_df = events_pipeline.read(input_dir)
        
        # Validate the raw data
        events_pipeline.validate(events_df)
        
        if validate_only:
            print("Validation completed successfully.")
            return events_df
        
        # Transform the data (includes session computation and enrichment)
        transformed_df = events_pipeline.transform(events_df)
        
        # Validate the transformed data
        events_pipeline.validate(transformed_df)
        
        # Write the final result
        events_pipeline.write(transformed_df, output_dir)
        
        print(f"Pipeline completed successfully. Results written to {output_dir}")
        return transformed_df


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
        return super().write(df, output_path)
    
    def validate(self, df: DataFrame) -> bool:
        return super().validate(df)
    

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
    
    def transform(self, df: DataFrame) -> DataFrame:
        # Step 1: Basic column transformations and filtering
        events_df = df.select(
            F.col("userid").alias("user_id"),
            F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("start_time"),
            F.col("traid").alias("track_id"),
            F.col("traname").alias("track_name"),
        ).filter(F.col("track_id").isNotNull())
        
        # Step 2: Read profiles data for enrichment
        profiles_path = os.path.join(
            os.path.dirname(os.path.dirname(df.inputFiles()[0])),  # Get base path
            LastFmFiles.PROFILES.value
        )
        profiles_df = (
            self.spark.read.option("header", True)
            .option("sep", "\t")
            .csv(profiles_path)
            .select(
                F.col("userid").alias("user_id"),
                F.col("country").alias("country"),
            )
        )
        
        # Step 3: Compute sessions for events
        sessions_df = self._compute_sessions(events_df)
        
        # Step 4: Get top sessions by track count
        top_sessions_df = self._get_top_sessions(sessions_df)
        
        # Step 5: Join events with profiles and recompute sessions
        events_with_profiles = events_df.join(profiles_df, on="user_id", how="left")
        sessions_with_profiles = self._compute_sessions(events_with_profiles)
        
        # Step 6: Get top sessions for enriched data
        top_sessions_enriched = self._get_top_sessions(sessions_with_profiles)
        
        # Step 7: Filter to top sessions and compute final result
        top_events = sessions_with_profiles.join(top_sessions_enriched, on="session_id", how="inner")
        
        # Step 8: Aggregate and enrich final result
        result_df = (
            top_events.groupBy("track_id", "track_name")
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
        
        return result_df
    
    def _compute_sessions(self, events_df: DataFrame) -> DataFrame:
        """Compute sessions based on 20-minute gap between consecutive plays."""
        w = Window.partitionBy("user_id").orderBy("start_time")
        
        prev_start = F.lag("start_time").over(w)
        
        with_prev = events_df.withColumn("prev_start", prev_start)
        
        with_flags = with_prev.withColumn(
            "is_new_session",
            F.when(
                (F.col("prev_start").isNull()) | 
                (F.col("start_time").cast("long") - F.col("prev_start").cast("long") > SessionConfig.SESSION_GAP_MINUTES.value * 60), 
                1
            ).otherwise(0),
        )
        
        sessions = with_flags.withColumn(
            "session_seq",
            F.sum("is_new_session").over(w.rowsBetween(Window.unboundedPreceding, 0)),
        )
        
        sessions = sessions.withColumn(
            "session_id", F.concat_ws(":", F.col("user_id"), F.col("session_seq"))
        ).drop("prev_start", "is_new_session", "session_seq")
        
        return sessions
    
    def _get_top_sessions(self, sessions_df: DataFrame) -> DataFrame:
        """Get top sessions by track count."""
        session_track_counts = (
            sessions_df.groupBy("session_id").agg(F.countDistinct("track_id").alias("tracks_count"))
        )
        
        window_top = Window.orderBy(F.col("tracks_count").desc(), F.col("session_id"))
        top_sessions = session_track_counts.withColumn(
            "rank", F.row_number().over(window_top)
        ).filter(F.col("rank") <= SessionConfig.TOP_SESSIONS_COUNT.value).drop("rank")
        
        return top_sessions

    def write(self, df: DataFrame, output_path: str):
        # Write as TSV with header
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
        
        # Also write a convenient .tsv
        tsv_path = os.path.join(os.path.dirname(output_path), "top_songs.tsv")
        df.coalesce(1).write.mode("overwrite").option("header", True).option("sep", "\t").csv(tsv_path + ".dir")
    
    def validate(self, df: DataFrame) -> bool: #TODO threshold as parameters
        
        gdf = SparkDFDataset(df)

        # Expectations
        gdf.expect_column_values_to_not_be_null("user_id")
        gdf.expect_column_values_to_not_be_null("start_time")
        gdf.expect_column_values_to_not_be_null("track_id")
        gdf.expect_column_values_to_not_be_null("track_name")

        gdf.expect_column_values_to_match_regex("track_id", r"^.{1,}$")
        gdf.expect_column_values_to_be_of_type("start_time", "TimestampType")

        # Basic sanity: timestamps after 2000 and before 2030
        gdf.expect_column_values_to_be_between(
            "start_time", min_value="2000-01-01 00:00:00", max_value="2030-01-01 00:00:00"
        )

        # Reasonable track_name length
        gdf.expect_column_value_lengths_to_be_between("track_name", min_value=1, max_value=500)

        results = gdf.validate(result_format="SUMMARY")
        if not results.get("success", False):
            raise RuntimeError("Great Expectations validation failed: %s" % results)