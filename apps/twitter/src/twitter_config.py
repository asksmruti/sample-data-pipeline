import configparser


class TwitterConfig:
    """ Class that loads twitter CONSUMER_KEY and ACCESS_TOKEN from configuration.ini """
    config = configparser.ConfigParser()
    config.read("../twitter_configuration.ini")
    CONSUMER_KEY = config['TWITTER']['CONSUMER_KEY']
    CONSUMER_SECRET = config['TWITTER']['CONSUMER_SECRET']
    ACCESS_TOKEN = config['TWITTER']['ACCESS_TOKEN']
    ACCESS_TOKEN_SECRET = config['TWITTER']['ACCESS_TOKEN_SECRET']


class AWSConfig:
    """ Class that loads AWS KEY/SECRET and BUCKET from configuration.ini """
    config = configparser.ConfigParser()
    config.read("../twitter_configuration.ini")
    CREDENTIAL_PROFILE = config['AWS']['CREDENTIAL_PROFILE']
    S3_BUCKET = config['AWS']['S3_BUCKET']
    S3_PATH = config['AWS']['S3_PATH']
    REGION = config['AWS']['REGION']
