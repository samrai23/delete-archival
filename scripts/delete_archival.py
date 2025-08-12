import sys
import ast
import logging
import re
import subprocess
import json
from google.cloud import storage
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from datetime import date, timedelta
import glob
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("List Old Tables") \
    .enableHiveSupport() \
    .getOrCreate()


def read_config(file_path):
    # Initialize GCP storage client (project ID will be inferred from the environment)
    client = storage.Client()

    # Extract bucket name and object path
    file_path = file_path.replace("gs://", "")
    input_bucket_name = file_path.split("/")[0]
    input_path = "/".join(file_path.split("/")[1:])

    # Retrieve the blob
    bucket = client.get_bucket(input_bucket_name)
    blob = bucket.blob(input_path)

    # Download the JSON file as string and parse it
    config = json.loads(blob.download_as_string())
    return config


#method to drop tables older than retention time along with cleaning data from bucket
def delete_old_tables(dates_to_keep, path):
    # Extract database and table name from the path
    database_name, table_pattern = path.split('.', 1)

    # Find all tables matching the pattern
    tables_df = spark.sql(f"SHOW TABLES IN {database_name} LIKE '{table_pattern}'")
    tables = tables_df.select("tableName").rdd.map(lambda row: row[0]).collect()

    logger.info("Deleting from: {}".format(path))

    # Get the timestamp of each table and filter older tables
    old_tables = []
    for table in tables:
        # Get the table creation timestamp
        timestamp_str = spark.sql(f"DESCRIBE FORMATTED {database_name}.{table}") \
            .filter("col_name = 'Created Time'") \
            .collect()[0].data_type

        timestamp = datetime.strptime(timestamp_str, '%a %b %d %H:%M:%S UTC %Y')
        if timestamp < datetime.strptime(dates_to_keep, '%Y-%m-%d'):
            old_tables.append(table)

            # Get the location of the table's data on GCS
            location = spark.sql(f"DESCRIBE FORMATTED {database_name}.{table}") \
                .filter("col_name = 'Location'") \
                .collect()[0].data_type

            # Drop the table from Hive
            spark.sql(f"DROP TABLE IF EXISTS {database_name}.{table}")

            # Remove the data from GCS using subprocess to call gsutil
            if location.startswith('gs://'):  # Ensure it's a GCS path
                try:
                    subprocess.run(['gsutil', '-m', 'rm', '-r', location], check=True)
                    logger.info(f"Deleted GCS data at location: {location}")
                except subprocess.CalledProcessError as e:
                    logger.error(f"Failed to delete data at location: {location}. Error: {str(e)}")

    logger.info("Deleted these old tables: {}".format(old_tables))

# method to get latest week from a parquet location
def get_week_parquet(path, date_table, duration, partition):
    try:
        # Get partition values from the GCP location
        weeks = spark.read.parquet(path).select(partition).distinct().rdd.map(lambda row: row[0]).collect()

        # Find the latest week value
        latest_week = max(weeks)

        # Get the date table
        df = spark.read.table(date_table)

        # Apply row number function over distinct column values
        windowSpec = Window.orderBy(df['fis_week_id'].desc())
        df_with_row_num = df.select('fis_week_id').distinct().withColumn("row_num", row_number().over(windowSpec))

        # Find the row number corresponding to the latest week
        given_row_num = \
            df_with_row_num.filter(df_with_row_num['fis_week_id'] == latest_week).select("row_num").collect()[0][
                "row_num"]

        # Calculate the adjusted row number
        adjusted_row_num = given_row_num + duration - 1

        # Retrieve the fis_week value corresponding to the adjusted row number
        res_fis_week_row = \
            df_with_row_num.filter(df_with_row_num["row_num"] == adjusted_row_num).select('fis_week_id').collect()[0]
        res_fis_week = int(res_fis_week_row['fis_week_id'])

        # Return the result
        logger.info("week limit to retain:  {}".format(res_fis_week))
        return res_fis_week

    except Exception as e:
        return str(e)


#method to delete parquet files having week partitions
def delete_parquet_week_partitions(path, weeks_to_keep, partition):
    try:
        # Initialize GCP storage client
        client = storage.Client()

        # Retrieve bucket name and prefix from the path
        bucket_name = path.split('/')[2]
        prefix = '/'.join(path.split('/')[3:])

        partition_values = []

        # List blobs in the bucket with the specified prefix
        blobs = client.list_blobs(bucket_name, prefix=prefix)
        for blob in blobs:
            # Extract the partition value from the blob name
            value = blob.name.split('/')[-2].split('=')[-1]

            # Check if the partition value is numeric
            if value.isdigit() and int(value) < weeks_to_keep:
                logger.info("Deleting partition: {}={}".format(partition, value))
                # Delete the blob
                blob.delete()

        # Iterate over blobs and delete those older than weeks_to_keep
        logger.info("Deleting partitions older than {} from path {}".format(weeks_to_keep, path))

    except Exception as e:
        logger.info(f"An error occurred: {str(e)}")


# method to delete parquet files with date partition
def delete_parquet_date_partitions(path, weeks_to_keep, partition):
    try:
        # Initialize GCP storage client
        client = storage.Client()

        # Retrieve bucket name and prefix from the path
        bucket_name = path.split('/')[2]
        prefix = '/'.join(path.split('/')[3:])

        logger.info("Deleting partitions older than {} from path {}".format(weeks_to_keep, path))
        if '-' in weeks_to_keep:
            weeks_to_keep_dt = datetime.strptime(weeks_to_keep, '%Y-%m-%d')
            weeks_to_keep = int(weeks_to_keep_dt.strftime('%Y%m%d'))
        else:
            weeks_to_keep = int(weeks_to_keep)

        # List blobs in the bucket with the specified prefix
        blobs = client.list_blobs(bucket_name, prefix=prefix)
        for blob in blobs:
            # Extract the partition value from the blob name
            value = blob.name.split('/')[-2].split('=')[-1]
            value = value.replace('-', '')
            # Check if the partition value is numeric
            if value.isdigit() and int(value) < weeks_to_keep:
                logger.info("Deleting partition: {}={}".format(partition, value))
                # Delete the blob
                blob.delete()


    except Exception as e:
        logger.info(f"An error occurred: {str(e)}")

# helper method to fetch the required retention week before which cleanup is required
def get_week_hive(path, date_table, duration):
    try:
        database_name, table_name = path.split('.', 1)
        partitions_df = spark.sql(f"SHOW PARTITIONS {database_name}.{table_name}")
        partition_values = partitions_df.select("partition").rdd.map(lambda row: row[0]).collect()
        latest_partition = sorted(partition_values, reverse=True)[0]
        partition_week = latest_partition.split("=")[1]
        #logger.info("Latest Partition Value:".format(partition_week))

        column_name = 'fis_week_id'
        df = spark.table(date_table)

        # Apply row number function over distinct column values
        windowSpec = Window.orderBy(df[column_name].desc())
        df_with_row_num = df.select(column_name).distinct().withColumn("row_num", row_number().over(windowSpec))

        # Find the row number corresponding to the given fis_week value
        given_row_num = \
        df_with_row_num.filter(df_with_row_num[column_name] == partition_week).select("row_num").collect()[0]["row_num"]
        # Calculate the adjusted row number
        adjusted_row_num = given_row_num + duration - 1

        # Retrieve the fis_week value corresponding to the adjusted row number
        res_fis_week_row = \
        df_with_row_num.filter(df_with_row_num["row_num"] == adjusted_row_num).select(column_name).collect()[0]
        res_fis_week = int(res_fis_week_row[column_name])

        # Show the result
        logger.info("week limit to retain:  {}".format(res_fis_week))
        return res_fis_week


    except Exception as e:
        return str(e)

#method to delete week partitions from hive
def delete_hive_week_partitions(path, weeks_to_keep):
    try:
        # Extract database and table name from the path
        database_name, table_name = path.split('.')

        # Get the partitions from the table
        partitions_df = spark.sql(f"SHOW PARTITIONS {database_name}.{table_name}")
        logger.info("partitions present in this hive table: {}".format(path))
        partitions_df.show()

        # Construct filter condition to extract numeric value after '='
        condition = f"int(SUBSTR(partition, INSTR(partition, '=') + 1)) < {weeks_to_keep}"

        # Filter partitions based on weeks_to_keep
        partitions_to_delete = partitions_df.filter(condition)

        # Print the partitions to delete
        logger.info("dropping these partitions from: {}".format(path))
        partitions_to_delete.show(truncate=False)

        # deleting partiitons from the hive table
        partitions_to_drop = partitions_to_delete.selectExpr("partition").rdd.flatMap(lambda x: x).collect()

        for partition in partitions_to_drop:
            spark.sql(f"ALTER TABLE {database_name}.{table_name} DROP IF EXISTS PARTITION ({partition})")


    except Exception as e:
        logger.info(f"An error occurred: {str(e)}")


# method to delete date partitions from hive
def delete_hive_date_partitions(path, weeks_to_keep):
    try:
        # Extract database and table name from the path
        database_name, table_name = path.split('.')

        # Convert weeks_to_keep to a datetime object
        weeks_to_keep_dt = datetime.strptime(weeks_to_keep, '%Y-%m-%d')

        # Get the partitions from the table
        partitions_df = spark.sql(f"SHOW PARTITIONS {database_name}.{table_name}")
        logger.info("Partitions present in this hive table: {}".format(path))
        partitions_df.show()

        # Construct filter condition to extract numeric value after '='
        partitions_to_delete = partitions_df.filter(
            f"to_date(SUBSTR(partition, INSTR(partition, '=') + 1)) < '{weeks_to_keep}'"
        )

        # Delete partitions from the hive table
        partitions_to_drop = partitions_to_delete.selectExpr("partition").rdd.flatMap(lambda x: x).collect()

        for partition in partitions_to_drop:
            # Properly quote the date in the SQL statement
            partition_value = partition.split('=')[-1]
            spark.sql(f"ALTER TABLE {database_name}.{table_name} DROP IF EXISTS PARTITION ({partition.split('=')[0]}='{partition_value}')")

        # Print the partitions to delete
        logger.info("Dropping these partitions from: {}".format(path))
        partitions_to_delete.show(truncate=False)

    except Exception as e:
        logger.info(f"An error occurred: {str(e)}")


# method to get required week from rollup scores location
def get_week_files(path, date_table, duration):
    try:
        # List directories at the specified path
        list_command = f"gsutil ls -d {path}/scores_*"
        logger.info(f"Executing command: {list_command}")

        # Run the gsutil command and capture the output
        result = subprocess.run(list_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Check for errors in the command execution
        if result.returncode != 0:
            logger.error(f"gsutil command failed with error: {result.stderr.decode('utf-8')}")
            return

        output = result.stdout.decode('utf-8').splitlines()

        # Extract subdirectories matching the pattern scores_<week>
        subdirs = []
        for line in output:
            match = re.search(r'scores_(\d+)', line)
            if match:
                subdir = match.group(1)  # Extract the numeric week part
                subdirs.append(subdir)

        logger.info(f"weeks in this path: {subdirs}")

        if not subdirs:
            logger.info("No subdirectories found.")
            return

        # Find the latest fis_week
        latest_week = max(int(week) for week in subdirs)
        logger.info(f"Latest fis_week: {latest_week}")

        # Get the date table
        df = spark.read.table(date_table)

        # Apply row number function over distinct column values
        windowSpec = Window.orderBy(df['fis_week_id'].desc())
        df_with_row_num = df.select('fis_week_id').distinct().withColumn("row_num", row_number().over(windowSpec))

        # Find the row number corresponding to the latest week
        given_row_num = \
            df_with_row_num.filter(df_with_row_num['fis_week_id'] == latest_week).select("row_num").collect()[0][
                "row_num"]

        # Calculate the adjusted row number
        adjusted_row_num = given_row_num + duration - 1

        # Retrieve the fis_week value corresponding to the adjusted row number
        res_fis_week_row = \
            df_with_row_num.filter(df_with_row_num["row_num"] == adjusted_row_num).select('fis_week_id').collect()[0]
        res_fis_week = int(res_fis_week_row['fis_week_id'])

        # Return the result
        logger.info("week limit to retain:  {}".format(res_fis_week))
        return res_fis_week

    except Exception as e:
        return str(e)



# method to delete files from a location
def delete_files(path, dates_to_keep=None, fis_week=None):
    try:

        logger.info(f"Deleting files from: {path}")

        if ('category_scores' in path or 'category_brand_scores' in path or 'subcategory_scores' in path) and fis_week:
            # List directories at the specified path
            list_command = f"gsutil ls -d {path}/scores_*"
            logger.info(f"Executing command: {list_command}")

            # Run the gsutil command and capture the output
            result = subprocess.run(list_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            # Check for errors in the command execution
            if result.returncode != 0:
                logger.error(f"gsutil command failed with error: {result.stderr.decode('utf-8')}")
                return

            output = result.stdout.decode('utf-8').splitlines()

            # Extract subdirectories matching the pattern scores_<week>
            subdirs = []
            for line in output:
                match = re.search(r'scores_\d+', line)
                if match:
                    subdir = match.group(0)
                    subdirs.append(subdir)

            logger.info(f"Subdirectories in this path: {subdirs}")

            for subdir in subdirs:
                week = subdir.split('_')[1]
                if fis_week and int(week) < int(fis_week):
                    full_path = f"{path}/{subdir}"
                    logger.info(f"Deleting subdirectory {full_path} older than fis_week {fis_week}")
                    delete_command = f"gsutil -m rm -r {full_path}"
                    logger.info(f"Executing command: {delete_command}")
                    subprocess.run(delete_command, shell=True, check=True)

        else:

            # Initialize GCP storage client
            client = storage.Client()
            # Retrieve bucket and blob name from the path
            path_components = path.split('/')
            bucket_name = path_components[2]
            blob_prefix = '/'.join(path_components[3:])

            # Get bucket
            bucket = client.get_bucket(bucket_name)
            blobs = list(bucket.list_blobs(prefix=blob_prefix))

            if dates_to_keep:
                dates_to_keep_naive = dates_to_keep.replace(tzinfo=None)

                for blob in blobs:
                    blob_time = blob.time_created.replace(tzinfo=None)

                    if blob_time < dates_to_keep_naive:
                        logger.info(f"Deleting blob {blob.name} created on {blob_time}")
                        blob.delete()
            else:
                logger.error("No date or fiscal week provided for deletion criteria.")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")


# method to get latest date from dates table
def get_latest_date(table):
    df = spark.read.table(table)
    latest_date_row = df.select('date_short_name').orderBy(F.desc('date_short_name')).limit(1).collect()[0]
    latest_date = date.fromisoformat(latest_date_row['date_short_name'])
    return latest_date


# method to get latest week from dates table
def get_latest_week(table):
    df = spark.read.table(table)
    latest_week_row = df.select('fis_week_id').distinct().orderBy(F.desc('fis_week_id')).limit(1).collect()[0]
    latest_week = int(latest_date_row['fis_week_id'])
    return latest_week


# main method to delete the data based on conditions provided in config
def delete_archival_data(config_file_path):
    config = read_config(config_file_path)
    date_table = config.get("date_table")

    # Iterate over each section in the config
    for section_name, section_data in config.get("archival_list").items():
        path = section_data["path"]
        archival_cadence = section_data["archival_cadence"]
        duration = int(section_data["duration"])
        partition = section_data.get("partition")

        if "/" in path:  # Parquet file
            if archival_cadence == "week":
                if partition != "":
                    logger.info('\n')
                    weeks_to_keep = get_week_parquet(path, date_table, duration, partition)
                    delete_parquet_week_partitions(path, weeks_to_keep, partition)
                else:
                    if ('category_scores' in path or 'category_brand_scores' in path or 'subcategory_scores' in path):
                        fis_week = get_week_files(path, date_table, duration)
                        delete_files(path, fis_week=fis_week)
                    else:
                        current_date = datetime.now()
                        dates_to_keep = current_date - timedelta(days=duration * 7)
                        logger.info('\n')
                        logger.info("date limit to retain:  {}".format(dates_to_keep.strftime('%Y-%m-%d')))
                        delete_files(path, dates_to_keep=dates_to_keep)

            elif archival_cadence == "day":
                if partition != "":
                    # partition on the basis of date_id
                    current_date = datetime.now()
                    dates_to_keep = current_date - timedelta(days=duration)
                    logger.info('\n')
                    logger.info("date limit to retain:  {}".format(dates_to_keep.strftime('%Y-%m-%d')))
                    dates_to_keep = dates_to_keep.strftime('%Y-%m-%d')
                    delete_parquet_date_partitions(path, dates_to_keep, partition)
                else:
                    current_date = datetime.now()
                    dates_to_keep = current_date - timedelta(days=duration)
                    logger.info('\n')
                    logger.info("date limit to retain:  {}".format(dates_to_keep.strftime('%Y-%m-%d')))
                    delete_files(path, dates_to_keep=dates_to_keep)

        else:  # Hive table
            if archival_cadence == "week":
                if partition != "":
                    logger.info('\n')
                    weeks_to_keep = get_week_hive(path, date_table, duration)
                    delete_hive_week_partitions(path, weeks_to_keep)

                else:  # no partition but we have duration as weeks so we multiply duration by 7
                    current_date = datetime.now()
                    dates_to_keep = current_date - timedelta(days=duration * 7)
                    logger.info('\n')
                    logger.info("date limit to retain:  {}".format(dates_to_keep.strftime('%Y-%m-%d')))
                    delete_old_tables(dates_to_keep, path)

            elif archival_cadence == "day":
                if partition != "":
                    current_date = datetime.now()
                    dates_to_keep = current_date - timedelta(days=duration)
                    logger.info('\n')
                    logger.info("date limit to retain:  {}".format(dates_to_keep.strftime('%Y-%m-%d')))
                    dates_to_keep = dates_to_keep.strftime('%Y-%m-%d')
                    delete_hive_date_partitions(path, dates_to_keep)

                else:
                    current_date = datetime.now()
                    dates_to_keep = current_date - timedelta(days=duration)
                    logger.info('\n')
                    logger.info("date limit to retain:  {}".format(dates_to_keep.strftime('%Y-%m-%d')))
                    dates_to_keep = dates_to_keep.strftime('%Y-%m-%d')
                    delete_old_tables(dates_to_keep, path)


if __name__ == "__main__":
    config_file_path = sys.argv[1]
    logger.info("reading config from: {}".format(config_file_path))
    delete_archival_data(config_file_path)