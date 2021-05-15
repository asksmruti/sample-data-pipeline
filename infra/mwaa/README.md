# Terraform AWS MWAA Setup
This repo will enable you to deploy the AWS managed airflow.


## Variables
Below is an example `terraform.tfvars` file that you can use in your deployments:

```ini
region               = "eu-central-1"
prefix               = "demo-mwaa"
vpc_cidr             = "10.0.1.0/24"
azs                  = ["eu-central-1a", "eu-central-1b"]
public_subnet_cidrs  = ["10.0.1.0/26", "10.0.1.64/26"]
private_subnet_cidrs = ["10.0.1.128/26", "10.0.1.192/26"]
mwaa_max_workers     = 2
```

## DAGs

There's a test DAG file inside the local [`dags` directory](dags)

You can place as many DAG files inside that directory as you want and Terraform will pick them up and upload them to S3.

## Usage

```bash
cp -r ../../apps/* dags/
make init
make plan
make apply
make destroy
```