

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.pipelines.lastfm.pipeline import EventsPipeline
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder         .master("local")         .appName("test-lastfm-pipeline")         .getOrCreate()

@pytest.fixture
def events_pipeline(spark):
    return EventsPipeline(spark)

def test_transformation_1(spark, events_pipeline):
    schema = StructType([
        StructField("userid", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("traid", StringType(), True),
        StructField("traname", StringType(), True),
    ])
    data = [
        ("user1", "2025-01-01T00:00:00Z", "track1_id", "track1_name"),
        ("user1", "2025-01-01T00:10:00Z", None, None),
    ]
    df = spark.createDataFrame(data, schema)
    
    result_df = events_pipeline._transformation_1(df)
    
    assert result_df.count() == 1
    assert "start_time" in result_df.columns
    assert result_df.schema["start_time"].dataType == TimestampType()
    assert result_df.collect()[0]["track_id"] == "track1_id"

def test_transformation_2(spark, events_pipeline):
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("track_id", StringType(), True),
        StructField("track_name", StringType(), True),
    ])
    data = [
        ("user1", "2025-01-01 00:00:00", "track1", "Track 1"),
        ("user1", "2025-01-01 00:15:00", "track2", "Track 2"),
        ("user1", "2025-01-01 00:40:00", "track3", "Track 3"), # New session
        ("user2", "2025-01-01 01:00:00", "track4", "Track 4"),
    ]
    df = spark.createDataFrame(data, schema)

    result_df = events_pipeline._transformation_2(df)

    assert "session_id" in result_df.columns
    
    sessions = result_df.collect()
    assert sessions[0]["session_id"] == "user1:1"
    assert sessions[1]["session_id"] == "user1:1"
    assert sessions[2]["session_id"] == "user1:2"
    assert sessions[3]["session_id"] == "user2:1"

def test_transformation_3(spark, events_pipeline, monkeypatch):
    schema = StructType([
        StructField("session_id", StringType(), True),
        StructField("track_id", StringType(), True),
    ])
    data = [
        ("session1", "track1"),
        ("session1", "track2"),
        ("session2", "track3"),
        ("session3", "track4"),
        ("session3", "track5"),
        ("session3", "track6"),
    ]
    df = spark.createDataFrame(data, schema)
    
    # Mock TOP_SESSIONS_COUNT to 2 for testing
    from src.constants import SessionConfig
    monkeypatch.setattr(SessionConfig, "TOP_SESSIONS_COUNT", type("Enum", (), {"value": 2}))

    result_df = events_pipeline._transformation_3(df)
    
    assert result_df.count() == 2
    
    top_sessions = [row["session_id"] for row in result_df.collect()]
    assert "session3" in top_sessions
    assert "session1" in top_sessions
    assert "session2" not in top_sessions

def test_transformation_4(spark, events_pipeline):
    sessions_schema = StructType([
        StructField("session_id", StringType(), True),
        StructField("track_id", StringType(), True),
    ])
    sessions_data = [
        ("session1", "track1"),
        ("session1", "track2"),
        ("session2", "track3"),
    ]
    sessions_df = spark.createDataFrame(sessions_data, sessions_schema)
    
    top_sessions_schema = StructType([
        StructField("session_id", StringType(), True),
        StructField("tracks_count", StringType(), True),
    ])
    top_sessions_data = [("session1", "2")]
    top_sessions_df = spark.createDataFrame(top_sessions_data, top_sessions_schema)

    result_df = events_pipeline._transformation_4(sessions_df, top_sessions_df)
    
    assert result_df.count() == 2
    assert result_df.filter(col("session_id") == "session1").count() == 2
    assert result_df.filter(col("session_id") == "session2").count() == 0
