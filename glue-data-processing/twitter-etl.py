#########################################
### IMPORT LIBRARIES AND SET VARIABLES
#########################################

import sys
from datetime import datetime, timedelta
# Import pyspark modules
from pyspark.context import SparkContext
import pyspark.sql.functions as f
from pyspark.sql.types import DataType, StructType, ArrayType
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
import boto3
from botocore.exceptions import ClientError


# Import glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.job import Job


# Setting hadoop configuration to avoid temporary file creation
sc = SparkContext()
sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

#########################################
### EXTRACT (READ DATA)
#########################################
def extract_data(s3_read_path: str, file_format: str) -> DataFrame:
    """
    Create Dynamicframe
    :param s3_read_path: S3 path
    :param file_format: Input file format
    :return: Dataframe
    """
    print("Extracting files...")
    print("S3 read paths: ", s3_read_path)

    # Read data to Glue dynamic frame
    try:
        dynamic_frame_read = glue_context.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [s3_read_path], "recurse": True},
            format=file_format,
            format_options={"withHeader": True},
            transformation_ctx="datasource0"
        )
    except ValueError:
        print("Unable to read dynamic frame")

    print("Schema of dynamic dataframe", dynamic_frame_read.printSchema())
    print("Total number of records in dataframe", dynamic_frame_read.count())

    if dynamic_frame_read.count() == 0:
        print("No new files found")
        return False
    else:
        return dynamic_frame_read


#########################################
### TRANSFORM (MODIFY DATA)
#########################################
def transform_data(transform_data_frame):
    """
    Data Transformation
    :param transform_data_frame: Dataframe
    :param enable_flatten: Flatten nested columns
    :param flatten_cols: List of cloumns to be flattened
    :return:  Transformed dateframe
    """
    print("Transformation started...")
    # Remove null field
    df_without_null = DropNullFields.apply(frame=transform_data_frame)
    # Convert dynamic frame to data frame to use standard pyspark functions
    df = df_without_null.toDF()

    try:
        df = df.dropDuplicates()
        df = df.withColumn("CreatedDate", to_date(df["created_at"], "EEE MMM dd HH:mm:ss Z yyyy"))
    except ValueError:
        print("Unable to transform data")

    print("Row count after deleting duplicates: ", df.count())
    print("Schema of dataframe after transformation: ", df.printSchema())

    return df


#########################################
### LOAD (WRITE DATA)
#########################################
def load_data(load_data_frame, s3_write_path, file_format,
              partition_col, num_partitions):
    """
    :param load_data_frame: Dataframe
    :param s3_write_path: S3 write path for processed data
    :param file_format: Output fileformat
    :param partition_col: Partition column
    :param num_partitions
    """
    remove_empty_str = "".join(partition_col.split())
    partition_col_list = remove_empty_str.split(",")  # Convert partition col string to list

    print("Loading files...")
    df = load_data_frame

    # Create [num_partitions] partitions
    data_frame_aggregated = df.repartition(num_partitions)

    # Convert back to dynamic frame
    dynamic_frame_write = DynamicFrame.fromDF(data_frame_aggregated,
                                              glue_context,
                                              "dynamic_frame_write")
    try:
        glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame_write,
            connection_type="s3",
            connection_options={"path": s3_write_path, "partitionKeys": partition_col_list},
            format=file_format
        )
    except ValueError:
        print("File loading failed")

    except ValueError:
        print("File loading failed")

    print("Schema of dynamic dataframe", dynamic_frame_write.printSchema())
    print("Total records written: ", dynamic_frame_write.toDF().count())
    print("ETL job completed")


#########################################
### CRAWLER TO CREATE SCHEMA
#########################################
def run_crawler(crawler_name):
    try:
        session = boto3.session.Session()
        glue_client = session.client('glue')
        glue_client.start_crawler(Name=crawler_name)

    except ClientError as e:
        raise Exception("boto3 client error for crawler: " + e.__str__())


if __name__ == '__main__':
    print("Twitter job start time:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # Input Parameters
    args = getResolvedOptions(sys.argv,
                              [
                                  'S3_INPUT_PATH',
                                  'S3_OUTPUT_PATH',
                                  'READ_FILE_FORMAT',
                                  'WRITE_FILE_FORMAT',
                                  'NUM_PARTITIONS',
                                  'PARTITION_COL',
                                  'CRAWLER_NAME',
                                  'JOB_NAME'
                              ])

    s3_input_path = args['S3_INPUT_PATH']
    s3_write_path = args['S3_OUTPUT_PATH']
    read_file_format = args['READ_FILE_FORMAT']
    write_file_format = args['WRITE_FILE_FORMAT']
    num_partitions = int(args['NUM_PARTITIONS'])
    partition_col = args['PARTITION_COL']
    crawler_name = args['CRAWLER_NAME']

    # Initialize glue contexts
    spark_context = SparkContext \
        .getOrCreate()
    glue_context = GlueContext(spark_context)
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)

    # Start ETL modules
    e_data_frame = extract_data(s3_input_path, read_file_format)

    # Exit if no new data found
    if e_data_frame:
        t_data_frame = transform_data(e_data_frame)
        load_data(t_data_frame, s3_write_path, write_file_format,
                  partition_col, num_partitions)
        run_crawler(crawler_name)

    # ETL Completed
    print("Twitter job end time:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    job.commit()
