// The glue role name - Provide all glue related permission
variable "GLUE_ROLE" {}

// ETL ETL script path in S3
variable "SCRIPT_PATH" {}

// S3 Path where to read data
variable "S3_INPUT_PATH" {}

// S3 path to write data
variable "S3_OUTPUT_PATH" {}

// Partitioning field
variable "PARTITION_COL" {}

// Crawler Name
variable "CRAWLER_NAME" {}

## Default parameters
// Raw file format eg. json, csv etc..
variable "READ_FILE_FORMAT" { default = "json" }

// Number of Partitions
variable "NUM_PARTITIONS" { default = 1 }

// Write file format eg. parquet
variable "WRITE_FILE_FORMAT" { default = "parquet" }

// Set the script language
variable "LANG" { default = "python" }
