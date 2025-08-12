# Delete Archival Workflow

This repository automates the archival and deletion of old data from Hive tables and Google Cloud Storage (GCS) buckets using PySpark and Google Dataproc.

## Repository Structure

- `configs/config.json`  
  Configuration file specifying tables, GCS paths, archival cadence, retention duration, and partition columns.

- `scripts/delete_archival.py`  
  Main PySpark script for deleting old tables, partitions, and files based on the configuration.

- `scripts/timestamp_script.py`  
  Python script to generate a timestamp for workflow runs.

- `workflows/delete-archival.json`  
  Workflow definition for orchestrating the archival process using Python and Dataproc tasks.

## Workflow Overview

1. **Get Current Date**  
   Runs [`scripts/timestamp_script.py`](scripts/timestamp_script.py) to generate a timestamp variable (`CALENDAR_WEEK`).

2. **Create Dataproc Cluster**  
   Provisions a Dataproc cluster with custom initialization actions and Spark/Hive configurations.

3. **Delete Archival Task**  
   Executes [`scripts/delete_archival.py`](scripts/delete_archival.py) on the Dataproc cluster, passing the config file as an argument.  
   The script:
   - Reads the config from GCS.
   - For each entry, determines which data to delete based on cadence (`week` or `day`), duration, and partition.
   - Deletes old Hive tables, partitions, or GCS files accordingly.

4. **Delete Dataproc Cluster**  
   Tears down the Dataproc cluster after completion.

## Configuration

Edit [`configs/config.json`](configs/config.json) to specify:
- `date_table`: Table containing date/week metadata.
- `archival_list`: List of objects with:
  - `path`: Hive table or GCS path.
  - `archival_cadence`: `"week"` or `"day"`.
  - `duration`: Retention period.
  - `partition`: Partition column (if applicable).

## Running the Workflow

The workflow is defined in [`workflows/delete-archival.json`](workflows/delete-archival.json) and can be triggered by your orchestrator (e.g., Argo, Airflow).

### Manual Execution

To run the archival script manually on Dataproc:

```sh
spark-submit scripts/delete_archival.py configs/config.json
```

## Requirements

- Google Cloud Dataproc
- PySpark
- Google Cloud Storage Python client
- Proper IAM permissions for Dataproc and GCS

## Logging

All actions and errors are logged using Python's logging module.

## License

See [LICENSE](LICENSE) for details.