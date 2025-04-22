# Import necessary libraries for your ETL pipeline
from databricks.sdk.runtime import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
import time
import uuid

# Logging function to capture the start time of pipeline execution
def on_execution_start(pipeline_name):
    start_time = time.time()  # Capture the start time (timestamp)
    print(f"Pipeline {pipeline_name} started at {start_time}")
    return start_time

# Logging function to capture the end time of pipeline execution
def on_execution_end(start_time, pipeline_name):
    end_time = time.time()  # Capture the end time (timestamp)
    execution_duration = end_time - start_time  # Calculate the duration
    print(f"Pipeline {pipeline_name} completed in {execution_duration} seconds")

def set_uuid():
    global unique_id
    unique_id = uuid.uuid4()
    print(str(unique_id))

def get_notebook_path():
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    notebook_name = notebook_path.split("/")[-1]

    return notebook_path

def combine_files():
    # Define the schema for `w.details`
    details_schema = StructType([
        StructField("path", StringType(), True)  # Assuming `details` contains a JSON with `path`
    ])

    # Run SQL Query to join all logs
    df = spark.sql("""
        SELECT 
            s.uuid AS `uuid`,
            s.source_file AS `source`,
            s.file_format,
            s.additional_info AS `read_parameters`,
            s.timestamp AS `read_timestamp`,
            t.transformation_name,
            t.details AS `modification_details`,
            t.timestamp AS `modification_timestamp`,
            w.action_name AS `write_action`,
            w.details AS `write_details`,
            w.timestamp AS `write_timestamp`     
        FROM delta.`/mnt/datalake/Pipeline1/source1/source_logs/` AS s 
        INNER JOIN delta.`/mnt/datalake/Pipeline1/transformation1/trans_logs/` AS t 
        ON s.uuid = t.uuid 
        INNER JOIN delta.`/mnt/datalake/Pipeline1/destination1/write_logs/` AS w 
        ON s.uuid = w.uuid
    """)

    # Convert `w.details` (JSON string) into a struct and extract `path`
    df = df.withColumn("destination", from_json(col("write_details"), details_schema).getField("path"))

    # Write the transformed data to Delta Lake
    df.write.mode("append").option("mergeSchema", "True").format("delta").save("/mnt/datalake/Pipeline1/lineage/lineage_logs/")


def combine_sql_file():
    from pyspark.sql.functions import col, expr, when, lit
    from delta.tables import DeltaTable

    # Read new data from SQL logs
    new_df = spark.read.format("delta").load("/mnt/datalake/Pipeline1/sql/sql_logs/")

    # Define the target path
    target_path = "/mnt/datalake/Pipeline1/lineage/lineage_logs/"

    if DeltaTable.isDeltaTable(spark, target_path):
       
        target_table = DeltaTable.forPath(spark, target_path)

        target_table.alias("target").merge(
            new_df.alias("source"),
            "target.uuid = source.uuid" 
        ).whenNotMatchedInsert(values={
            "source": col("source.source_tables"),
            "modification_details": col("source.query"),
            "modification_timestamp": col("source.timestamp"),
            "destination": col("source.destination_tables") 
        }).execute()

    else:
        new_df.write.format("delta").mode("overwrite").save(target_path)

    lineage_df = spark.read.format("delta").load("/mnt/datalake/Pipeline1/lineage/lineage_logs/")
    source_log_df = spark.read.format("delta").load("/mnt/datalake/Pipeline1/Mount_And_External_Sources/file_write_log/")

    source_log_df = source_log_df.withColumn(
        "normalized_file_path",
        expr("CASE WHEN file_path LIKE 'dbfs:%' THEN file_path ELSE concat('dbfs:', file_path) END")
    )

    lineage_df = lineage_df.drop("origin")  # in case origin already exists

    joined_df = lineage_df.alias("lineage").join(
        source_log_df.alias("src"),
        expr("lineage.source LIKE concat(src.normalized_file_path, '%')"),
        "left"
    ).withColumn(
        "origin",
        when(col("src.source_type").isNotNull(), col("src.source_type")).otherwise(lit("DBFS"))
    ).withColumn(
        "notebook_path",
        when(col("lineage.notebook_path").isNotNull(), col("lineage.notebook_path")).otherwise(lit(get_notebook_path()))
    ).drop("normalized_file_path", "source_type", "file_path", "timestamp", "notebook")


    joined_df.write.mode("overwrite").option("mergeSchema", "true").format("delta").save("/mnt/datalake/Pipeline1/lineage/lineage_logs/")


from pyspark.sql import DataFrameReader
from pyspark.sql import SparkSession
import os
from datetime import datetime
import uuid

# Global variable to store log directory path
logs_dir = "/mnt/datalake/source_logs/"  # Default value

# Function to set logs directory path dynamically
def set_source_logs_dir(path):
    global logs_dir
    logs_dir = path
    print(f"Logs directory set to: {logs_dir}")

# Wrapper class around the DataFrameReader to intercept file read operations
class CustomDataFrameReader(DataFrameReader):
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.spark = spark

    def _log_source_details(self, file_path, file_format, additional_info=None):
        global logs_dir

        # Ensure the logs directory exists (create if not present)
        try:
            # Check if the directory exists, create if not
            if not any(dbutils.fs.ls(logs_dir)):
                dbutils.fs.mkdirs(logs_dir)  # Create the directory if it doesn't exist
            print(f"Log directory {logs_dir} is ready.")
        except Exception as e:
            print(f"Error while creating directory {logs_dir}: {e}")
        
        
        # Metadata to log
        metadata = {
            'uuid': str(unique_id),
            'source_file': file_path,
            'file_format': file_format,
            'additional_info': str(additional_info) if additional_info else '',
            'timestamp': str(datetime.now())
        }
        
        # Create a DataFrame for the source log and write it to Delta
        lineage_df = self.spark.createDataFrame([metadata])
        
        try:
            lineage_df.write.format("delta").mode("append").save(logs_dir)
            print(f"Source log successfully written for {file_path} to {logs_dir}")
        except Exception as e:
            print(f"Error while writing to {logs_dir}: {e}")

    def csv(self, path, **options):
        """Overriding the CSV read method to include logging"""
        # Log source details
        self._log_source_details(path, 'csv', options)
        
        # Call the original CSV read method
        return super().csv(path, **options)

    def parquet(self, path, **options):
        """Overriding the Parquet read method to include logging"""
        # Log source details
        self._log_source_details(path, 'parquet', options)
        
        # Call the original Parquet read method
        return super().parquet(path, **options)

    def format(self, source_format):
        """Overriding format method to capture format type"""
        self._current_format = source_format
        return super().format(source_format)
    
    def load(self, path=None, **options):
        """Overriding load method to log details before reading"""
        file_format = getattr(self, "_current_format", "unknown")
        if path:
            self._log_source_details(path, file_format, options)
        return super().load(path, **options)

# Override the default DataFrameReader with our custom one
def wrap_spark_reader(spark: SparkSession):
    spark._read = CustomDataFrameReader(spark)
    return spark

# Initialize the SparkSession and apply the wrapper
spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()
wrap_spark_reader(spark)

from pyspark.sql import DataFrame
from datetime import datetime
import pyspark.sql.functions as F

# Global variable to store log directory path
transformation_logs_dir = "/mnt/datalake/trans_logs/"  # Default value

# Function to set logs directory path dynamically
def set_trans_logs_dir(path):
    global transformation_logs_dir
    transformation_logs_dir = path
    print(f"Logs directory set to: {transformation_logs_dir}")

# Function to log transformations
# Global variable to track if the directory has been created
log_directory_created = False  

def log_transformation(transformation_name, details):
    global transformation_logs_dir, log_directory_created

    if not log_directory_created:
        try:
            dbutils.fs.ls(transformation_logs_dir)  # Check if directory exists
        except Exception:
            dbutils.fs.mkdirs(transformation_logs_dir)  # Create if not exists
            print(f"Log directory {transformation_logs_dir} is ready.")
        log_directory_created = True  # Mark as created to avoid repeated checks

    metadata = {
        'uuid': str(unique_id),
        'transformation_name': transformation_name,
        'details': str(details),
        'timestamp': str(datetime.now())
    }
    lineage_df = spark.createDataFrame([metadata])
    lineage_df.write.format("delta").mode("append").save(transformation_logs_dir)


# Class to log and wrap DataFrame transformations
class CapturingDataFrame:
    def __init__(self, spark_df, pipeline_name):
        self._df = spark_df  # Store the original DataFrame
        self.pipeline_name = pipeline_name  # Store the pipeline name

    def _log_and_apply(self, transformation_name, *args, **kwargs):
        """Helper function to log and apply transformations."""
        log_transformation(transformation_name, {"args": args, "kwargs": kwargs})
        return getattr(self._df, transformation_name)(*args, **kwargs)

    # Overriding DataFrame transformations
    def filter(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("filter", *args, **kwargs), self.pipeline_name)

    def groupBy(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("groupBy", *args, **kwargs), self.pipeline_name)

    def agg(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("agg", *args, **kwargs), self.pipeline_name)

    def withColumnRenamed(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("withColumnRenamed", *args, **kwargs), self.pipeline_name)

    def select(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("select", *args, **kwargs), self.pipeline_name)

    def join(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("join", *args, **kwargs), self.pipeline_name)

    def withColumn(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("withColumn", *args, **kwargs), self.pipeline_name)

    def drop(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("drop", *args, **kwargs), self.pipeline_name)

    def dropDuplicates(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("dropDuplicates", *args, **kwargs), self.pipeline_name)

    def distinct(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("distinct", *args, **kwargs), self.pipeline_name)

    def orderBy(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("orderBy", *args, **kwargs), self.pipeline_name)

    def limit(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("limit", *args, **kwargs), self.pipeline_name)

    def cache(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("cache", *args, **kwargs), self.pipeline_name)

    def persist(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("persist", *args, **kwargs), self.pipeline_name)

    def repartition(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("repartition", *args, **kwargs), self.pipeline_name)

    def coalesce(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("coalesce", *args, **kwargs), self.pipeline_name)

    def fillna(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("fillna", *args, **kwargs), self.pipeline_name)

    def dropna(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("dropna", *args, **kwargs), self.pipeline_name)

    def transform(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("transform", *args, **kwargs), self.pipeline_name)

    def alias(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("alias", *args, **kwargs), self.pipeline_name)

    def crossJoin(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("crossJoin", *args, **kwargs), self.pipeline_name)

    def union(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("union", *args, **kwargs), self.pipeline_name)

    def unionByName(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("unionByName", *args, **kwargs), self.pipeline_name)

    def intersect(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("intersect", *args, **kwargs), self.pipeline_name)

    def exceptAll(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("exceptAll", *args, **kwargs), self.pipeline_name)

    def intersectAll(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("intersectAll", *args, **kwargs), self.pipeline_name)

    def repartitionByRange(self, *args, **kwargs):
        return CapturingDataFrame(self._log_and_apply("repartitionByRange", *args, **kwargs), self.pipeline_name)

    # Proxy methods for other DataFrame operations can be added as needed
    def __getattr__(self, attr):
        """For any other DataFrame methods that are not explicitly wrapped, 
        use the original DataFrame methods."""
        return getattr(self._df, attr)

# Wrapper function to capture the initial DataFrame and pipeline name
def wrap_dataframe(df, pipeline_name):
    return CapturingDataFrame(df, pipeline_name)


from pyspark.sql import DataFrame
from datetime import datetime
import pyspark.sql.functions as F

# Global variables
write_logs_dir = "/mnt/datalake/write_logs/"  # Default log directory
logs_initialized = False  # Flag to track directory initialization

# Function to set logs directory path dynamically
def set_write_logs_dir(path):
    global write_logs_dir, logs_initialized
    write_logs_dir = path
    logs_initialized = False  # Reset the flag when path changes
    print(f"Logs directory set to: {write_logs_dir}")

# Function to initialize log directory (only once)
def initialize_log_directory():
    global logs_initialized
    if logs_initialized:
        return  # Skip if already initialized

    try:
        # Check if the directory exists, create if not
        if not any(dbutils.fs.ls(write_logs_dir)):
            dbutils.fs.mkdirs(write_logs_dir)  # Create the directory if it doesn't exist
            print(f"Log directory {write_logs_dir} created.")
        else:
            print(f"Log directory {write_logs_dir} already exists.")
    except Exception as e:
        print(f"Error while creating directory {write_logs_dir}: {e}")

    logs_initialized = True  # Set flag to prevent redundant execution

# Function to log write actions
def log_write_action(action_name, details):
    initialize_log_directory()  # Ensure the log directory is initialized only once

    metadata = {
        'uuid': str(unique_id),
        'action_name': action_name,
        'details': str(details),
        'timestamp': str(datetime.now())
    }

    # Log into a Delta table (or any other logging system)
    lineage_df = spark.createDataFrame([metadata])
    lineage_df.write.format("delta").mode("append").save(write_logs_dir)

# Class to wrap DataFrame and capture write actions
class CapturingWriteDataFrame:
    def __init__(self, spark_df, pipeline_name):
        self._df = spark_df
        self.pipeline_name = pipeline_name  # Store the pipeline name
        self._write_mode = None  # This will hold the write mode
        self._write_path = None  # This will hold the write path

    def _log_write_and_apply(self, action_name, *args, **kwargs):
        """Helper function to log write actions."""
        log_write_action(action_name, {"args": args, "kwargs": kwargs, "path": self._write_path})
        # Perform the actual write operation
        return getattr(self._df, action_name)(*args, **kwargs)

    def save(self, *args, **kwargs):
        """Override the save method to log the write operation."""
        return self._log_write_and_apply("save", *args, **kwargs)

    def insertInto(self, *args, **kwargs):
        """Override insertInto method for tables to log the write operation."""
        return self._log_write_and_apply("insertInto", *args, **kwargs)

    # Handle 'mode' method separately to capture the write mode
    def mode(self, mode):
        """Override mode method to capture the write mode."""
        self._write_mode = mode
        return self  # Return the current instance to allow chaining

    # Capture the path (location) where the data is being written
    def path(self, path):
        """Method to set the write path and log it."""
        self._write_path = path
        return self  # Return self to allow method chaining

    # Capture path for the specific write functions (parquet, csv, etc.)
    def _capture_path(self, *args, **kwargs):
        """Capture path from any arguments or kwargs passed."""
        if args:
            self._write_path = args[0]  # Capture from args if it's the first argument
        elif 'path' in kwargs:
            self._write_path = kwargs['path']

    # Intercept and capture paths for all specific write functions like parquet, csv, etc.
    def parquet(self, *args, **kwargs):
        """Override parquet write method to capture the path."""
        self._capture_path(*args, **kwargs)
        return self._log_write_and_apply("parquet", *args, **kwargs)

    def csv(self, *args, **kwargs):
        """Override csv write method to capture the path."""
        self._capture_path(*args, **kwargs)
        return self._log_write_and_apply("csv", *args, **kwargs)

    def json(self, *args, **kwargs):
        """Override json write method to capture the path."""
        self._capture_path(*args, **kwargs)
        return self._log_write_and_apply("json", *args, **kwargs)

    # New `line_write` method to accept and log path
    def line_write(self, path=None, *args, **kwargs):
        """Custom write method to handle DataFrame write operations with mode."""
        # If path is provided explicitly, use it
        if path:
            self._write_path = path
        
        # Log the write action before actually writing the data
        log_write_action("line_write", {
            "mode": self._write_mode, 
            "df_schema": str(self._df.schema),
            "path": self._write_path
        })
        
        writer = self._df.write
        if self._write_mode:
            writer = writer.mode(self._write_mode)

        # Check if the path is provided or set previously
        if self._write_path:
            # Log the actual write operation with path
            return writer.parquet(self._write_path, *args, **kwargs)  # Assuming parquet, but can be adjusted for other formats
        else:
            return writer.parquet(*args, **kwargs)  # If no path, still write without a path

    # Proxy methods for other DataFrame operations (excluding transformations)
    def __getattr__(self, attr):
        """For any other DataFrame methods that are not explicitly wrapped, 
        use the original DataFrame methods."""
        return getattr(self._df, attr)

# Wrapper function to capture the initial DataFrame and pipeline name
def wrap_write_dataframe(df, pipeline_name):
    return CapturingWriteDataFrame(df, pipeline_name)

import re
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Define log directory for combined source and destination logs
log_dir = "/mnt/datalake/sql_activity_logs/"  # Log path for both source and destination

# Initialize the Spark session
spark = SparkSession.builder.appName("SQLLogger").getOrCreate()

def set_sql_logs_dir(path):
    global log_dir
    log_dir = path
    print(f'Sql logs dir set to: {log_dir}')

def initialize_log_directory():
    """
    Initializes log directory for both source and destination tables if it doesn't exist.
    """
    try:
        # Check if the directory exists by listing the parent directory
        dbutils.fs.mkdirs(log_dir)  # This creates the directory if it doesn't exist
        print(f"Directory created or already exists: {log_dir}")
    except Exception as e:
        print(f"Error while creating directory {log_dir}: {str(e)}")

def log_sql_activity(query, source_tables, destination_tables):
    """
    Log SQL activity, including both source and destination tables, and the query that was executed.
    """
    # Generate a unique UUID for the current query
    uuid_for_query = str(uuid.uuid4())
    timestamp = str(datetime.now())
    
    # Combine source and destination tables
    log_metadata = [{
        'uuid': uuid_for_query,
        'query': query,
        'source_tables': source_tables,
        'destination_tables': destination_tables,
        'timestamp': timestamp
    }]
    
    # Define schema explicitly
    schema = StructType([
        StructField("uuid", StringType(), True),
        StructField("query", StringType(), True),
        StructField("source_tables", StringType(), True),
        StructField("destination_tables", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    # Create DataFrame with explicit schema
    log_df = spark.createDataFrame(log_metadata, schema)
    
    # Write log information to a Delta table (append mode)
    log_df.write.format("delta").mode("append").save(log_dir)


def execute(query):
    initialize_log_directory()
    return SQLExecutor(spark).execute_sql(query)

class SQLExecutor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def execute_sql(self, query):
        """
        Execute the SQL query, track source and destination tables, and log the activity.
        """
        # Extract source and destination tables from the query
        source_tables, destination_tables = self._extract_tables_from_query(query)
        
        # If no destination tables found (like in UPDATE queries), log as "unknown"
        if not destination_tables:
            destination_tables = ["unknown"]
        
        # Log the source and destination tables along with the query
        log_sql_activity(query, source_tables, destination_tables)
        
        # Execute the SQL query and return the result
        return self.spark.sql(query)

    def _extract_tables_from_query(self, query):
        """
        Extract source and destination tables from the SQL query using regular expressions.
        """
        # Regex to find tables mentioned in 'FROM' or 'JOIN'
        source_tables = re.findall(r'FROM\s+([a-zA-Z0-9_]+)|JOIN\s+([a-zA-Z0-9_]+)', query)
        source_tables = [table for sublist in source_tables for table in sublist if table]

        # Regex to find tables mentioned in 'INSERT INTO' or 'CREATE TABLE AS' or 'CREATE TABLE IF NOT EXISTS'
        destination_tables = re.findall(r'(?:INSERT\s+INTO|CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?)\s+([a-zA-Z0-9_]+)|CREATE\s+TABLE\s+AS\s+([a-zA-Z0-9_]+)', query)
        destination_tables = [table for sublist in destination_tables for table in sublist if table]

        # For UPDATE or other queries where destination is not specified, mark as 'unknown'
        if 'UPDATE' in query:
            destination_tables = []

        return source_tables, destination_tables

#set the path as required by user
def set_log_path(source = "/mnt/datalake/source_logs/",
                 trans = "/mnt/datalake/trans_logs/",
                 dest = "/mnt/datalake/write_logs/",
                 sql =  "/mnt/datalake/sql_activity_logs/"):
    set_source_logs_dir(source)
    set_trans_logs_dir(trans)
    set_write_logs_dir(dest)
    set_sql_logs_dir(sql)

#Read the object and return the wrapped dataframe
class DefObject:
    def __init__(self, otype, pipeline_name):
        self.otype = otype
        self.pipeline_name = pipeline_name
        set_uuid()

    def read_object(self, query = None, file_path = None, file_type = None, **options):
        if self.otype.lower() == "file":
            if file_type.lower() == "csv":
                odf = spark._read.csv(file_path, **options)  # Use _read to access the custom reader
                df = wrap_dataframe(odf, self.pipeline_name) # wrap dataframe
                return df

            elif file_type.lower() == "parquet":
                odf = spark._read.parquet(file_path, **options)  # Use _read to access the custom reader
                df = wrap_dataframe(odf, self.pipeline_name) # wrap dataframe
                return df

            elif file_type.lower() == "avro":
                odf = spark._read.format("avro").load(file_path, **options)  # Use _read to access the custom reader
                df = wrap_dataframe(odf, self.pipeline_name) # wrap dataframe
                return df

        elif self.otype.lower() == "table":
            execute(query)

class WriteObject:
    def __init__(self, save_type, pipeline_name):
        self.pipeline_name = pipeline_name
        self.save_type = save_type

    def write_object(self, df, file_path, mode):
        if self.save_type.lower() == "file":
            # Write to destination (this will automatically log the write action)
            df_write = wrap_write_dataframe(df, self.pipeline_name)
            df_write.mode(mode).line_write(file_path)

            print(f"File written successfully at path: {file_path} with {mode} mode")
            combine_files()

        elif self.save_type.lower() == "table":
            pass

#Combined File
def get_lineage_data():
    combine_sql_file()
    lineage_logs = spark.read.format("delta").load("/mnt/datalake/Pipeline1/lineage/lineage_logs/")

    lineage_logs.sort(F.col("read_timestamp").desc()).display(truncate=False)

from pyspark.sql.functions import col

def get_lineage(object_name):
    lineage_logs = spark.read.format("delta").load("/mnt/datalake/Pipeline1/lineage/lineage_logs/")

    filtered_logs = lineage_logs.filter(lineage_logs['destination'] == object_name)

    result_logs = filtered_logs.select(
        col('destination').alias('object_name'),
        col('notebook_path'),
        col('source'),
        col('origin')
    ).distinct()

    result_logs.display(truncate=False)