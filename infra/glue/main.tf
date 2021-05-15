data "aws_iam_role" "GLUE_ROLE" {
  name = var.GLUE_ROLE
}

resource "aws_glue_job" "twitter-etl" {
  glue_version = "2.0"
  name         = "twitter-etl"
  description  = "Glue ETL job"
  role_arn     = data.aws_iam_role.GLUE_ROLE.arn
  max_capacity = 2.0
  max_retries  = 1
  timeout      = 60

  command {
    name            = "glueetl"
    script_location = var.SCRIPT_PATH
    python_version  = "3"
  }

  default_arguments = {
    "--enable-glue-datacatalog" = "true"
    "--job-bookmark-option"     = "job-bookmark-enable"
    "--job-language"            = var.LANG
    "--enable-metrics"          = "true"
    "--S3_OUTPUT_PATH"          = var.S3_OUTPUT_PATH
    "--S3_INPUT_PATH"           = var.S3_INPUT_PATH
    "--READ_FILE_FORMAT"        = var.READ_FILE_FORMAT
    "--WRITE_FILE_FORMAT"       = var.WRITE_FILE_FORMAT
    "--PARTITION_COL"           = var.PARTITION_COL
    "--NUM_PARTITIONS"          = var.NUM_PARTITIONS
    "--CRAWLER_NAME"            = var.CRAWLER_NAME
  }
  execution_property {
    max_concurrent_runs = 1
  }
}

## Create database & crawler
resource "aws_glue_catalog_database" "twitter_db" {
  name = "twitter-db"
}

resource "aws_glue_crawler" "twitter_crawler" {
  database_name = aws_glue_catalog_database.twitter_db.name
  name          = "twitter-crawler"
  role          = data.aws_iam_role.GLUE_ROLE.arn
  s3_target {
    path = var.S3_OUTPUT_PATH
  }
}