region               = "eu-central-1"
prefix               = "twitter-mwaa"
vpc_cidr             = "10.0.1.0/24"
azs                  = ["eu-central-1a", "eu-central-1b"]
public_subnet_cidrs  = ["10.0.1.0/26", "10.0.1.64/26"]
private_subnet_cidrs = ["10.0.1.128/26", "10.0.1.192/26"]
mwaa_max_workers     = 2