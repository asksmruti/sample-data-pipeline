import boto3
import argparse
from botocore.exceptions import ClientError
from twitter_config import AWSConfig


def run_glue_job(job_name):
    config = AWSConfig()
    session = boto3.session.Session(profile_name=config.CREDENTIAL_PROFILE, region_name=config.REGION)
    glue_client = session.client('glue')
    try:
        job_run_id = glue_client.start_job_run(JobName=job_name)
        return job_run_id
    except ClientError as e:
        raise Exception("boto3 client error in run_glue_job: " + e.__str__())
    except Exception as e:
        raise Exception("Unexpected error in run_glue_job: " + e.__str__())


def usage():
    parser = argparse.ArgumentParser()
    parser.add_argument('-j',
                        '--job-name',
                        type=str,
                        nargs='?',
                        default=None,
                        required=True,
                        help="Glue job name")
    return parser.parse_args()


def main():
    argparse_obj = usage()
    jobId = run_glue_job(job_name=argparse_obj.job_name)
    print(jobId)


if __name__ == "__main__":
    main()
