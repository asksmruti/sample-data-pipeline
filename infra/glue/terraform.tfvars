GLUE_ROLE      = "ROLE_NAME"
SCRIPT_PATH    = "S3://<GLUE_S3_SCRIPT_PATH>.py"
S3_INPUT_PATH  = "S3://<PATH_WHERE_RAW_DATA_IS_STORED>/"
S3_OUTPUT_PATH = "S3://<PATH_WHERE_PROCESSED_DATA_WILLBE_STORED>/"
PARTITION_COL  = "CreatedDate" ## This is purely use-case based
CRAWLER_NAME   = "twitter-crawler"