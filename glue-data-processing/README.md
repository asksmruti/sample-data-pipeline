### Sample glue etl job

This glue etl job has 4 stages -

1. Extract or read data from S3 by using glue dynamic dataframe
2. Transformation or modify certain columns(based on use-case), partitioning and cleaning
3. Load the snappy compressed parquest data into S3 
4. Run the glue crawler to create schema in glue catalog

#### Parameters:

The job needs following parameters:
```
GLUE_ROLE      = "ROLE_NAME" 
SCRIPT_PATH    = "S3://<GLUE_S3_SCRIPT_PATH>.py"
S3_INPUT_PATH  = "S3://<PATH_WHERE_RAW_DATA_IS_STORED>/" 
S3_OUTPUT_PATH = "S3://<PATH_WHERE_PROCESSED_DATA_WILLBE_STORED>/"
PARTITION_COL  = "CreatedDate" ## This is purely use-case based and depends on data. At the moment this is hardcoded to do column transformation
CRAWLER_NAME   = "<crawler_name>" 
```