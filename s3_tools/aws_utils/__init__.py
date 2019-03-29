import boto3
from s3_tools import settings


def get_aws_session(name=None, **kwargs):
    fields = ['profile_name', 'region_name', 'aws_access_key_id', 'aws_secret_access_key', 'aws_session_token']
    p = dict([(k, v) for k, v in kwargs.items() if k in fields])
    if not p and name:
        conf = settings.get('aws.{}'.format(name))
        if conf and isinstance(conf, dict):
            p.update(conf)
    return boto3.Session(**p)
