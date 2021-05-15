import boto3
import sys
from botocore.exceptions import ClientError
import logging
from pathlib import Path
from boto3.s3.transfer import TransferConfig

log = logging.getLogger('s3_uploader')
log.setLevel(logging.INFO)
log_format = logging.Formatter("%(asctime)s: - %(levelname)s: %(message)s", "%H:%M:%S")
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(log_format)
log.addHandler(stream_handler)

_THRESHOLD = 1  # Size threshold in GB
_CONCURRENCY = 5


def login(profile, region):
    try:
        session = boto3.session.Session(profile_name=profile, region_name=region)
        s3_client = session.client('s3')
    except ClientError as e:
        log.error(e)
    return s3_client


def multipart_upload_to_s3(profile, region, file, bucket, object_name):
    s3_client = login(profile, region)
    if not Path(file).is_file:
        log.error("File [" + file + "] does not exist!")
        raise FileNotFoundError

    if object_name is None:
        log.error("object_path is null!")
        raise ValueError("S3 object must be set!")

    GB = 1024 ** 3
    mp_threshold = _THRESHOLD * GB
    concurrency = _CONCURRENCY
    transfer_config = TransferConfig(multipart_threshold=mp_threshold,
                                     use_threads=True,
                                     max_concurrency=concurrency)

    try:
        s3_client.upload_file(file, bucket, object_name, Config=transfer_config)
        log.info(f"Batch file uploaded at s3://{bucket}/{object_name}",)
    except boto3.exceptions.S3UploadFailedError as e:
        log.error("Failed to upload object!")
        log.exception(e)
        if 'ExpiredToken' in str(e):
            log.warning('Login token expired')
            log.info("Handling...")
        else:
            log.error("Unknown error!")
