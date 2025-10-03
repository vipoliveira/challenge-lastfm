# LastFM Challenge Code Changes

This document outlines the changes made to the codebase to improve the LastFM challenge solution.

## Changes Made

### 1. Pipeline Structure
- Standardized `ProfilesPipeline` and `EventsPipeline` classes to strictly implement only the `BasePipeline` abstract methods
- Removed all helper methods from `EventsPipeline`, integrating their logic into the transform method
- Added detailed docstrings to all methods for better code readability and documentation
- Improved error handling in validation methods

### 2. Entrypoint Implementation
- Refactored the `Entrypoint` class to have three class methods: `validate_task`, `compute_task`, and `post_process_validation`
- Removed the `__init__` method and `run` method for a cleaner, more direct implementation
- Each task method creates its own Spark session with appropriate configuration
- Added detailed docstrings with type hints for better code readability
- Added post-processing validation to verify the quality of the results

### 3. Airflow DAG
- Uses `PythonOperator` optimized for Spark execution for all tasks
- Directly calls Entrypoint class methods with no wrapper functions or scripts
- Added a post-validation task to verify the results
- Configures appropriate resource allocations for each Spark task
- Eliminates all script dependencies for a cleaner architecture
- Uses a consistent, resource-aware approach for all tasks

## Execution Instructions

### 1. Docker Compose Setup
Follow the README instructions to set up the Docker environment:
```bash
mkdir -p assets/lastfm-dataset-1k
tar -xzf /path/to/lastfm-dataset-1K.tar.gz -C assets
mv assets/lastfm-dataset-1K assets/lastfm-dataset-1k 2>/dev/null || true
docker compose up --build -d
```

### 2. Running the DAG
Access Airflow UI at http://localhost:8080 (user: admin, pass: admin) and trigger the "lastfm_top_songs" DAG. The DAG now has three tasks:

1. **Validation Task**:
   - Uses `PythonOperator` optimized for Spark execution
   - Directly calls `Entrypoint.validate_task`
   - Validates input data quality using Great Expectations
   - Allocates 4GB memory and 2 CPU cores for Spark processing

2. **Computation Task**:
   - Uses `PythonOperator` optimized for Spark execution
   - Directly calls `Entrypoint.compute_task`
   - Processes data to find top songs in longest sessions
   - Allocates 8GB memory and 4 CPU cores for intensive Spark processing

3. **Post-Validation Task**:
   - Uses `PythonOperator` optimized for Spark execution
   - Directly calls `Entrypoint.post_process_validation`
   - Verifies the quality of the generated results
   - Allocates 4GB memory and 2 CPU cores for Spark processing

### 3. Testing the Entrypoint Directly
You can test the Entrypoint class methods directly using Python:
```python
from src.pipelines.lastfm.entrypoint import Entrypoint

# For validation only
Entrypoint.validate_task(input_dir="/path/to/lastfm-dataset-1k")

# For full computation
Entrypoint.compute_task(input_dir="/path/to/lastfm-dataset-1k", output_dir="/path/to/output")

# For post-validation
Entrypoint.post_process_validation(output_dir="/path/to/output")
```

### 4. Accessing Results
Results will be available in:
- `./output/top_songs/` (Spark CSV directory with part file)
- `./output/top_songs.tsv.dir/` (TSV directory with part file)

You can create a single TSV file with:
```bash
cp ./output/top_songs.tsv.dir/part-* ./output/top_songs.tsv
```

## Output Format
The output is a tab-separated file with the following columns:
- `track_id`: String identifier for the track
- `track_name`: Name of the track
- `plays`: Number of plays across the top 50 sessions
- `distinct_users`: Number of distinct users who played the track
- `countries`: Comma-separated list of countries for users who played the track