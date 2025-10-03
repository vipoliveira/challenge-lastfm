# LastFM Top Songs Challenge

This project identifies the top 10 songs played in the top 50 longest user sessions from the Last.fm 1K dataset using PySpark, orchestrated with Airflow.

## Problem Definition

If we define a user 'session' to be composed of one or more songs played by that user, where each song is started within 20 minutes of the previous song's start time, create a list of the top 10 songs played in the top 50 longest sessions by tracks count.

## Project Overview

The solution consists of a modular PySpark pipeline with the following components:

1. **EventsPipeline**: Processes listening events to identify user sessions and find the top 50 longest sessions
2. **ProfilesPipeline**: Processes user profile data to extract country information
3. **ResultsPipeline**: Combines session events with user profiles to identify the top 10 songs

All orchestration is handled by an Airflow DAG that calls the pipeline components directly.

## Prerequisites

- Docker and Docker Compose
- Last.fm 1K dataset (see Setup Instructions)
- 4GB+ of available RAM for Docker

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository-url>
cd challenge-lastfm
```

### 2. Download and Extract the Dataset

Download the Last.fm 1K dataset from [http://ocelma.net/MusicRecommendationDataset/lastfm-1K.html](http://ocelma.net/MusicRecommendationDataset/lastfm-1K.html) or use the Dropbox link below:

```bash
# Create assets directory
mkdir -p assets

# Download the dataset (if not already downloaded)
wget https://www.dropbox.com/s/82j7p69hzmxsbwr/lastfm-dataset-1K.tar.gz -O lastfm-dataset-1K.tar.gz

# Extract the dataset
tar -xzf lastfm-dataset-1K.tar.gz -C assets

# Ensure the folder name is lowercase with hyphen
mv assets/lastfm-dataset-1K assets/lastfm-dataset-1k 2>/dev/null || true
```

Verify that the following files exist:
- `./assets/lastfm-dataset-1k/userid-profile.tsv`
- `./assets/lastfm-dataset-1k/userid-timestamp-artid-artname-traid-traname.tsv`

### 3. Create Output Directory

```bash
mkdir -p output
```

## Running the Pipeline

### Using Docker Compose (Recommended)

1. **Build and Start the Airflow Container:**

```bash
docker compose up --build -d
```

2. **Access the Airflow UI:**
   - Navigate to [http://localhost:8080](http://localhost:8080)
   - Login with username: `admin` and password: `admin`

3. **Run the DAG:**
   - Find the `lastfm_top_songs` DAG in the Airflow UI
   - Trigger the DAG manually by clicking the "Play" button

4. **View the Results:**
   - After the DAG completes successfully, the results will be available in the `output` directory:
     - `./output/top_songs/` (Spark CSV directory with results)
     - `./output/top_songs.tsv.dir/` (convenience TSV directory)

### Using Local Python Environment (For Development)

1. **Create and Activate a Virtual Environment:**

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. **Install Dependencies:**

```bash
pip install -r docker/requirements.txt
```

3. **Run the Pipeline Manually:**

```bash
# Set environment variables
export PYTHONPATH=$PYTHONPATH:$(pwd)

# Run validation
python -c "from src.pipelines.lastfm.entrypoint import Entrypoint; Entrypoint.validate_task(input_dir='./assets')"

# Run computation
python -c "from src.pipelines.lastfm.entrypoint import Entrypoint; Entrypoint.compute_task(input_dir='./assets', events_output_dir='./output/events', profiles_output_dir='./output/profiles')"

# Run post-processing
python -c "from src.pipelines.lastfm.entrypoint import Entrypoint; Entrypoint.post_process_validation(events_path='./output/events', profiles_path='./output/profiles', output_dir='./output/top_songs')"
```

## Output Format

The final output is a tab-separated file with the following columns:

- `track_id`: Unique identifier for the track
- `track_name`: Name of the track
- `plays`: Number of times the track was played in the top 50 sessions
- `distinct_users`: Number of distinct users who played the track
- `countries`: Comma-separated list of countries where users played the track

## Project Structure

```
challenge-lastfm/
├── assets/                    # Dataset directory (mounted to container)
│   └── lastfm-dataset-1k/     # Last.fm dataset files
├── dags/                      # Airflow DAGs
│   └── lastfm_top_songs_dag.py  # Main DAG definition
├── docker/                    # Docker configuration
│   ├── airflow.Dockerfile     # Custom Airflow image with Spark
│   └── requirements.txt       # Python dependencies
├── output/                    # Output directory (mounted to container)
├── src/                       # Source code
│   ├── base.py                # Base pipeline class
│   ├── constants.py           # Configuration constants
│   ├── factory.py             # Factory for creating Spark sessions
│   └── pipelines/             # Pipeline implementations
│       └── lastfm/            # LastFM pipeline
│           ├── entrypoint.py  # Pipeline entrypoint class
│           └── pipeline.py    # Pipeline implementation classes
├── docker-compose.yml         # Docker Compose configuration
└── README.md                  # Project documentation
```

## Data Validation

We use Great Expectations to validate:

- Input data before processing
- Session data after transformation
- Final results before writing to disk

Validation checks include:
- Non-null values for required fields
- Proper data types
- Value ranges and limits
- Expected counts for result sets

## Technologies Used

- **PySpark**: For distributed data processing
- **Airflow**: For workflow orchestration
- **Great Expectations**: For data validation
- **Docker**: For containerization and environment isolation

## Clean Up

To stop and remove containers:

```bash
docker compose down -v
```

## Assumptions and Design Decisions

1. **Session Definition**: A session ends when there's a gap of more than 20 minutes between song plays.
2. **Session Length**: Sessions are ranked by the number of distinct tracks, not by duration.
3. **Top Songs Metric**: Songs are ranked by total play count within the top 50 sessions.
4. **Data Quality**: The pipeline includes validation to ensure data quality at each step.
5. **Country Information**: User country data is included for additional context but does not affect rankings.

## Potential Improvements

1. **Scalability**: For larger datasets, configure Spark to run in a distributed cluster.
2. **Monitoring**: Add more detailed logging and metrics collection.
3. **Testing**: Add unit and integration tests for pipeline components.
4. **Performance**: Optimize transformations for larger datasets with caching and partitioning.
5. **UI**: Develop a simple web UI for viewing and exploring results.