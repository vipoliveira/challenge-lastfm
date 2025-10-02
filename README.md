## Kraken Challenge: PySpark in Airflow (docker-compose)

This project computes the top 10 songs played within the top 50 longest user sessions (by tracks count) from the Last.fm 1K dataset using PySpark, orchestrated by Airflow running in Docker.

Definitions:
- A session is one or more songs started by the same user, where each song starts within 20 minutes of the previous song’s start time.
- Longest sessions are ranked by the number of distinct tracks in the session.

### Prerequisites
- Docker and Docker Compose installed
- Download the Last.fm 1K dataset tarball from the challenge link and extract it locally so that the repository contains:
  - `./assets/lastfm-dataset-1k/userid-profile.tsv`
  - `./assets/lastfm-dataset-1k/userid-timestamp-artid-artname-traid-traname.tsv`
  
If you currently have only the tarball, you can extract it into the expected assets directory like this:
```bash
mkdir -p assets/lastfm-dataset-1k
tar -xzf /path/to/lastfm-dataset-1K.tar.gz -C assets
# After extraction, ensure the folder name is lowercase with hyphen: lastfm-dataset-1k
mv assets/lastfm-dataset-1K assets/lastfm-dataset-1k 2>/dev/null || true
```

### Project layout
- `docker-compose.yml`: Brings up Airflow with Spark installed
- `docker/airflow.Dockerfile`: Custom Airflow image with Java and Spark
- `docker/requirements.txt`: Python deps including `pyspark`
- `dags/lastfm_top_songs_dag.py`: Airflow DAG
- `scripts/compute_top_songs.py`: PySpark job
- `scripts/validate_events_ge.py`: Great Expectations validation (Spark)
- `output/`: Host-mounted output directory for results

### Setup & Run
1) Verify dataset is present under `./assets/lastfm-dataset-1k/` as described above.
2) Build and start the stack (builds custom Airflow image with Java, Spark, GE, and Spark provider):
```bash
docker compose up --build -d
```
3) Access Airflow UI at `http://localhost:8080` (user: `admin`, pass: `admin`).
4) Unpause and trigger DAG `kraken_top_songs`.

The DAG will:
1) Validate input events (reading from `/opt/airflow/assets/lastfm-dataset-1k`) with Great Expectations.
2) Submit the PySpark job to compute the result via SparkSubmitOperator.

### Output
The PySpark job writes a TSV with header under the host-mounted `output/` directory.

Expected location on host after successful run:
- `./output/top_songs/` (Spark CSV directory with a single part file)
- `./output/top_songs.tsv.dir/` (convenience TSV directory containing a single part file). You can copy/rename the part file to `top_songs.tsv`.

Each row schema:
- `track_id` (string)
- `track_name` (string)
- `plays` (integer) — number of plays across the selected top 50 sessions
- `distinct_users` (integer) — distinct users who played the track within those sessions
- `countries` (string) — comma-separated distinct countries for those users

### Clean up
```bash
docker compose down -v
```

### Data Quality (Great Expectations)
We run a validation step using Great Expectations on the raw events before computation. Checks include:
- Non-null `user_id`, `start_time`, `track_id`, `track_name`
- `start_time` type is timestamp and within a reasonable range (2000-2030)
- Non-empty `track_id` and reasonable `track_name` length

If validation fails, the DAG stops before running the compute job.


