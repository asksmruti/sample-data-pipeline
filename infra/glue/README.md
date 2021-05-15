# Terraform AWS MWAA Setup
This repo will enable you to deploy the AWS Glue job and a scheduler


## Variables
Below is an example `terraform.tfvars` file that you can use in your deployments:

```ini
GLUE_ROLE      = "ROLE_NAME"
SCRIPT_PATH    = "S3://<GLUE_S3_SCRIPT_PATH>.py"
S3_INPUT_PATH  = "S3://<PATH_WHERE_RAW_DATA_IS_STORED>/"
S3_OUTPUT_PATH = "S3://<PATH_WHERE_PROCESSED_DATA_WILLBE_STORED>/"
PARTITION_COL  = "CreatedDate" ## This is purely use-case based and depends on data 
CRAWLER_NAME   = "twitter-crawler" 
```

## Usage

```bash
make init
make plan
make apply
make destroy
```