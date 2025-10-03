from enum import Enum


class AirflowConfig(Enum):
    DEFAULT_INPUT_DIR = "/opt/airflow/assets"
    DEFAULT_OUTPUT_DIR = "/opt/airflow/output"
    DATA_DIR = "/opt/airflow/data"
    DATASET_DIR = "lastfm-dataset-1k"
    
    # Pipeline-specific output directories
    EVENTS_OUTPUT_DIR = "/opt/airflow/output/events"
    PROFILES_OUTPUT_DIR = "/opt/airflow/output/profiles"
    RESULTS_OUTPUT_DIR = "/opt/airflow/output/top_songs"

class SparkConfig(Enum):
    SPARK_CONF = {"spark.sql.session.timeZone": "UTC"}
    SPARK_MASTER = "local[*]"
    SPARK_DEFAULT_APP_NAME = "DefaultSparkApp"


class LastFmFiles(Enum):
    EVENTS = "userid-timestamp-artid-artname-traid-traname.tsv"
    PROFILES = "userid-profile.tsv"


class SessionConfig(Enum):
    SESSION_GAP_MINUTES = 20
    TOP_SESSIONS_COUNT = 50
    TOP_SONGS_COUNT = 10
