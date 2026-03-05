# batch-processing-homework

Homework solution for Batch Processing

Problems: <https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/06-batch/homework.md>

1. The `spark.version` output is 4.1.1.

    Just run `batch-processing.ipynb` from the beginning until:

    ```python
    print(f"Spark version: {spark.version}")
    ```

2. The average size of the Parquet (ending with .parquet extension) Files that were created is 24.4MB (&asymp; 25MB).

    Continue `batch-processing.ipynb` run until:

    ```python
    import os

    output_path = "output/yellow_2025_11"
    parquet_files = [f for f in os.listdir(output_path) if f.endswith(".parquet")]
    sizes_mb = [os.path.getsize(os.path.join(output_path, f)) / (1024 * 1024) for f in parquet_files]
    average_size_mb = sum(sizes_mb) / len(sizes_mb)

    print("Average Parquet file size (MB):", average_size_mb)
    ```

3. There are 162,604 taxi trips on November 15, 2025.

    Continue the previous run until:

    ```python
    specific_count = spark \
        .sql("""
            SELECT COUNT(1)
            FROM trips_data
            WHERE DATE(tpep_pickup_datetime) = DATE '2025-11-15'                       
        """) \
        .show()
    ```

4. The longest trip is 90.6 hours.

    Continue the previous run until:

    ```python
    longest_trip = spark \
        .sql("""
            SELECT (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 3600 AS trip_hours
            FROM trips_data
            ORDER BY trip_hours DESC
            LIMIT 1;
        """) \
        .show()
    ```

5. Spark user interface runs on `http://localhost:4040`.

6. The name of the least frequent pickup location zone can be either Governor's Island/Ellis Island/Liberty Island or Arden Heights.

    Continue the previous run until finished.
