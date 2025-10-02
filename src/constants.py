from enum import Enum


class AirflowConfig(Enum):
    DEFAULT_INPUT_DIR = "/opt/airflow/assets"
    DEFAULT_OUTPUT_DIR = "/opt/airflow/output"
    DATA_DIR = "/opt/airflow/data"
    DATASET_DIR = "lastfm-dataset-1k"
    
    # Environment variables
    LASTFM_DIR_ENV = "LASTFM_DIR"
    OUTPUT_DIR_ENV = "OUTPUT_DIR"
    
    # Default paths
    DEFAULT_LASTFM_DIR = "/opt/airflow/assets/lastfm-dataset-1k"
    DEFAULT_OUTPUT_DIR = "/opt/airflow/output/top_songs"
    
    # Script paths
    VALIDATE_SCRIPT = "/opt/airflow/scripts/validate_events_ge.py"
    COMPUTE_SCRIPT = "/opt/airflow/scripts/compute_top_songs.py"


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
