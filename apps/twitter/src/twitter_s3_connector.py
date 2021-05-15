import os
import sys
import gzip
import json
import tweepy
import logging
import argparse
import shutil
from typing import List
from datetime import datetime
from twitter_config import TwitterConfig, AWSConfig
from upload_to_s3 import multipart_upload_to_s3

log = logging.getLogger('tweet_scrapper')
log.setLevel(logging.INFO)
log_format = logging.Formatter("%(asctime)s: %(levelname)s: %(message)s", "%H:%M:%S")
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(log_format)
log.addHandler(stream_handler)


class TweepyConfig:
    """ Class for creating tweepy API objects loaded with twitter API keys """

    def __init__(self, config: 'TwitterConfig'):
        self.auth = tweepy.OAuthHandler(config.CONSUMER_KEY, config.CONSUMER_SECRET)
        self.auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)

    def tweepy_api(self):
        return tweepy.API(self.auth, wait_on_rate_limit=True)


class TweetStreamListener(tweepy.StreamListener):
    """ Tweepy listener class that inherits from tweepy.StreamListener """

    def __init__(self, limit, config: 'AWSConfig') -> None:
        super().__init__()
        self.tweet_download_limit = limit
        self.tweet_download_count = 1
        self.bucket_name = config.S3_BUCKET
        self.path = config.S3_PATH
        self.profile = config.CREDENTIAL_PROFILE
        self.region = config.REGION

    @staticmethod
    def on_connect(**kwargs) -> None:
        log.info("Connected to the Twitter API now")

    def on_data(self, data):
        """ Read tweets as JSON objs and extract data """
        json_data = json.loads(data)
        file_name = f"batch-{datetime.today().strftime('%H-%M-%S')}.json.gz"
        object_name = f"{self.path}/{datetime.today().strftime('%Y-%m-%d')}/{file_name}"
        with gzip.open(f'data/{file_name}', 'a') as output:
            output.write(json.dumps(json_data).encode('utf8') + b"\n")
        self.tweet_download_count += 1
        if self.tweet_download_count > self.tweet_download_limit:
            multipart_upload_to_s3(bucket=self.bucket_name,
                                   object_name=object_name,
                                   profile=self.profile,
                                   region=self.region,
                                   file=f"data/{file_name}")
            try:
                shutil.rmtree('data')
            except shutil.Error as e:
                log.error("Unable to delete data dir")
            return False  # To disable data streaming

    @staticmethod
    def on_error(status_code, **kwargs):
        # returning False in on_error disconnects the stream
        if status_code == 420:
            print("Request rate limit reached")
            return False

        if status_code != 200:
            log.error("Couldn't connect to twitter API")
            return False


def download_tweets_by_filters(api,
                               aws_config,
                               track: List[str] = None,
                               locations: List[str] = None,
                               languages: List[str] = None,
                               size: int = None):
    _track = track if track is not None else []
    _locations = locations if locations is not None else []
    _languages = languages if languages is not None else ['en']
    _size = size if size is not None else 100

    customStreamListener = TweetStreamListener(_size, aws_config)
    customStream = tweepy.Stream(auth=api.auth, listener=customStreamListener)

    customStream.filter(track=_track,
                        locations=_locations,
                        languages=_languages)


def validate_and_return_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t',
                        '--track',
                        type=str,
                        nargs='?',
                        default=None,
                        help="Name of file containing keywords i.e. python")

    parser.add_argument('-l',
                        '--locations',
                        type=str,
                        nargs='?',
                        default=None,
                        help="Name of file containing geo-locations[longitude, latitude] eg. 20.26, 85.84 ")

    parser.add_argument('-s',
                        '--size',
                        type=int,
                        nargs='?',
                        default=None,
                        help="Number of records to download. Default size is 100")

    return parser.parse_args()


def validate_file_and_rtn_filter_list(filename):
    """ Function to validate file exists and generate a list of keywords"""
    if filename is None:
        return []
    with open(filename, "r") as file:
        kw_list = file.read()
        kw_list = kw_list.strip().split()
        if kw_list:
            return kw_list
    raise EOFError("{} is empty".format(filename))


def main():
    twitter_config = TwitterConfig()
    aws_config = AWSConfig()
    api = TweepyConfig(twitter_config).tweepy_api()
    argparse_obj = validate_and_return_args()
    if (argparse_obj.track is None
            and argparse_obj.locations is None):
        log.warn("No filters files selected. Please add them")
        return -1

    track_filter = validate_file_and_rtn_filter_list(argparse_obj.track)
    location_filter = validate_file_and_rtn_filter_list(argparse_obj.locations)

    log.info("Active filters are:")
    if argparse_obj.track:
        print(f"Keywords from {argparse_obj.track}")
    if argparse_obj.locations:
        print(f"Locations from {argparse_obj.locations}")

    os.makedirs("data", exist_ok=True)
    download_tweets_by_filters(api,
                               track=track_filter,
                               locations=location_filter,
                               languages=['en'],
                               size=int(argparse_obj.size),
                               aws_config=aws_config)


if __name__ == "__main__":
    main()
