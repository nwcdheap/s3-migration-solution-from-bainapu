"""
Wrapper for boto3 S3 client.

:Author: wuwentao <wuwentao@patsnap.com>

"""
import os
import json
from hsettings import Settings
from s3_tools.aws_utils import get_aws_session


class S3Resource:

    def __init__(self, settings, profile=None):
        self._settings = settings
        self._profile = profile or 'copy'
        self._client = get_aws_session(self._profile).client('s3')

    def copy_object(self, source_bucket, target_bucket, source_key, target_key, **kwargs):
        param = {
            'ACL': 'bucket-owner-full-control',
            'CopySource': {
                'Bucket': source_bucket,
                'Key': source_key
            },
            'Bucket': target_bucket,
            'Key': target_key,
            'MetadataDirective': 'COPY',
            'TaggingDirective': 'COPY'
        }
        if kwargs:
            param.update(kwargs)
        self.client.copy_object(**param)

    def copy(self, source_bucket, target_bucket, source_key, target_key, **kwargs):
        param = {
            'CopySource': {
                'Bucket': source_bucket,
                'Key': source_key
            },
            'Bucket': target_bucket,
            'Key': target_key
        }
        if 'aws.copy_source' in self.settings:
            conf = self.settings.get('aws.copy_source')
            if conf and isinstance(conf, dict):
                source_cli = get_aws_session('copy_source').client('s3')
                param['SourceClient'] = source_cli
        if param:
            param.update(kwargs)
        self.client.copy(**param)

    def download_object(self, bucket, key, filename=None):
        if not filename:
            tmp_dir = self.settings.get('migration.tmp_dir', 'tmp')
            if not os.path.exists(tmp_dir):
                os.makedirs(tmp_dir, exist_ok=True)
            filename = os.path.join(tmp_dir, os.path.basename(key))
        self.client.download_file(Bucket=bucket, Key=key, Filename=filename)
        return filename

    def upload_object(self, bucket, key, filename):
        self.client.upload_file(Bucket=bucket, Key=key, Filename=filename)
        return key

    def head_object(self, bucket, key, **kwargs):
        param = {
            'Bucket': bucket,
            'Key': key
        }
        if kwargs:
            param.update(kwargs)
        return self.client.head_object(**param)

    def get_object_tagging(self, bucket, key, **kwargs):
        param = {
            'Bucket': bucket,
            'Key': key
        }
        if kwargs:
            param.update(kwargs)
        return self.client.get_object_tagging(**param)

    def put_object(self, bucket, key, body, **kwargs):
        param = {
            'Bucket': bucket,
            'Key': key,
            'Body': body,
            'ACL': 'bucket-owner-full-control'
        }
        if kwargs:
            param.update(kwargs)
        return self.client.put_object(**param)

    def put_object_tagging(self, bucket, key, tagging, **kwargs):
        param = {
            'Bucket': bucket,
            'Key': key,
            'Tagging': {
                'TagSet': tagging
            }
        }
        if kwargs:
            param.update(kwargs)
        return self.client(**param)

    def list_objects(self, bucket, prefix=None, max_keys=10, ctoken=None):
        p = {
            'Bucket': bucket,
            'MaxKeys': max_keys,
            'FetchOwner': True
        }
        if prefix:
            p['Prefix'] = prefix
        if ctoken:
            p['ContinuationToken'] = ctoken
        return self.client.list_objects_v2(**p)

    def list_objects_all(self, bucket, prefix=None, batch_num=1000):
        ctoken = None
        has_next = True
        while has_next:
            res = self.list_objects(bucket=bucket, prefix=prefix, max_keys=batch_num, ctoken=ctoken)
            if 'IsTruncated' in res and res['IsTruncated']:
                ctoken = res['NextContinuationToken']
            else:
                has_next = False
            yield res['Contents']
        return

    def delete_object(self, bucket, key, **kwargs):
        param = {
            'Bucket': bucket,
            'Key': key
        }
        if kwargs:
            param.update(kwargs)
        return self.client.delete_object(**param)

    @property
    def client(self):
        return self._client

    @property
    def settings(self) -> Settings:
        return self._settings


def split_s3_path(s3_path):
    if s3_path.startswith('s3://'):
        p = s3_path[5:]
        index = p.index('/')
        bucket = p[:index]
        key = p[index + 1:]
        return bucket, key
    raise ValueError('Invalid s3 path {}'.format(s3_path))


class ManifestFile:

    def __init__(self, file_path):
        super().__init__()
        self._file_path = file_path
        self.source_bucket = ''
        self.dest_bucket = ''
        self.create_timestamp = ''
        self.file_format = ''
        self.file_schema = ''
        self.files = []
        self.parse(file_path)

    def parse(self, file_path):
        with open(file_path) as fp:
            obj = json.load(fp)
            self.source_bucket = obj['sourceBucket']
            self.dest_bucket = obj['destinationBucket']
            if ':' in self.dest_bucket:
                self.dest_bucket = self.dest_bucket[self.dest_bucket.rindex(':') + 1:]
            self.create_timestamp = obj['creationTimestamp']
            self.file_format = obj['fileFormat']
            self.file_schema = [s.strip() for s in obj['fileSchema'].split(',')]
            self.files = obj['files']
