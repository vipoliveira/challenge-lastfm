from src.factory import build_spark_session
from src.constants import SparkConfig, AirflowConfig
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.pipelines.lastfm.pipeline import EventsPipeline, ProfilesPipeline, ResultsPipeline
from src.logger import get_logger

logger = get_logger(__name__)


class Entrypoint:

    @classmethod
    def validate_task(cls, **kwargs) -> dict:
        input_dir = kwargs.get("input_dir", AirflowConfig.DEFAULT_INPUT_DIR.value)
        
        spark = build_spark_session(app_name="LastFM-Validation", 
                                    spark_config=SparkConfig.SPARK_CONF.value)
        
        logger.info("Validating events data...")
        events_pipeline = EventsPipeline(spark=spark)
        events_df = events_pipeline.read(input_dir)
        events_pipeline.validate(events_df)
        logger.info("Events data validation completed successfully.")
        
        logger.info("Validating profiles data...")
        profiles_pipeline = ProfilesPipeline(spark=spark)
        profiles_df = profiles_pipeline.read(input_dir)
        profiles_pipeline.validate(profiles_df)
        logger.info("Profiles data validation completed successfully.")
        
        return {
            "events_df": events_df,
            "profiles_df": profiles_df
        }
    
    @classmethod
    def compute_task(cls, **kwargs) -> dict:
        input_dir = kwargs.get("input_dir", AirflowConfig.DEFAULT_INPUT_DIR.value)
        events_output_dir = kwargs.get("events_output_dir", AirflowConfig.EVENTS_OUTPUT_DIR.value)
        profiles_output_dir = kwargs.get("profiles_output_dir", AirflowConfig.PROFILES_OUTPUT_DIR.value)
        
        spark = build_spark_session(app_name="LastFM-Computation", 
                                    spark_config=SparkConfig.SPARK_CONF.value)
        
        logger.info("Processing events data...")
        events_pipeline = EventsPipeline(spark=spark)
        events_df = events_pipeline.read(input_dir)
        events_pipeline.validate(events_df)
        processed_events_df = events_pipeline.transform(events_df)
        events_pipeline.validate(processed_events_df)
        events_pipeline.write(processed_events_df, events_output_dir)
        logger.info(f"Events processing completed. Results written to {events_output_dir}")
        
        logger.info("Processing profiles data...")
        profiles_pipeline = ProfilesPipeline(spark=spark)
        profiles_df = profiles_pipeline.read(input_dir)
        profiles_pipeline.validate(profiles_df)
        processed_profiles_df = profiles_pipeline.transform(profiles_df)
        profiles_pipeline.validate(processed_profiles_df)
        profiles_pipeline.write(processed_profiles_df, profiles_output_dir)
        logger.info(f"Profiles processing completed. Results written to {profiles_output_dir}")
        
        return {
            "events_path": events_output_dir,
            "profiles_path": profiles_output_dir
        }
    
    @classmethod
    def post_process_validation(cls, **kwargs) -> DataFrame:
        events_path = kwargs.get("events_path", AirflowConfig.EVENTS_OUTPUT_DIR.value)
        profiles_path = kwargs.get("profiles_path", AirflowConfig.PROFILES_OUTPUT_DIR.value)
        output_dir = kwargs.get("output_dir", AirflowConfig.RESULTS_OUTPUT_DIR.value)
        
        spark = build_spark_session(app_name="LastFM-ResultsProcessing", 
                                    spark_config=SparkConfig.SPARK_CONF.value)
        
        logger.info("Reading processed events and profiles data...")
        results_pipeline = ResultsPipeline(spark=spark)
        data_dict = results_pipeline.read(events_path=events_path, profiles_path=profiles_path)
        
        logger.info("Validating input data...")
        results_pipeline.validate(data_dict)
        
        logger.info("Computing top songs...")
        results_df = results_pipeline.transform(data_dict)
        
        logger.info("Validating results...")
        results_pipeline.validate(results_df)
        
        logger.info(f"Writing results to {output_dir}...")
        results_pipeline.write(results_df, output_dir)
        
        logger.info("\nResults summary:")
        logger.info(f"Number of top songs: {results_df.count()}")
        
        stats_df = results_df.agg(
            F.sum("plays").alias("total_plays"),
            F.avg("plays").alias("avg_plays_per_song"),
            F.max("plays").alias("max_plays"),
            F.min("plays").alias("min_plays"),
            F.avg("distinct_users").alias("avg_users_per_song")
        ).collect()[0]
        
        logger.info(f"Total plays across top songs: {stats_df['total_plays']}")
        logger.info(f"Average plays per song: {stats_df['avg_plays_per_song']:.2f}")
        logger.info(f"Plays range: {stats_df['min_plays']} to {stats_df['max_plays']}")
        logger.info(f"Average distinct users per song: {stats_df['avg_users_per_song']:.2f}")
        
        logger.info("\nTop 5 songs:")
        top5 = results_df.orderBy(F.col("plays").desc()).limit(5)
        top5.show(truncate=False)
        
        logger.info("Post-processing completed successfully.")
        return results_df