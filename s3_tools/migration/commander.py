"""
Commander package for s3 migration.

:Author: wuwentao <wuwentao@patsnap.com>

Commander is used to send migration tasks to SQS queues.
Commander use a lister object to get S3 file list.
- S3ObjectLister: use boto3 list objects API to get objects, easy to use for small buckets
- S3InventoryLister: use S3 inventory files to get objects, helpful for large buckets
"""
import os
import csv
import logging
import gzip
from urllib import parse
from s3_tools import settings
from s3_tools.aws_utils.s3 import split_s3_path, ManifestFile, S3Resource
from s3_tools.migration import SOURCE_BUCKET_KEY, TARGET_BUCKET_KEY, KEYS_KEY, SOURCE_KEY_KEY, TARGET_KEY_KEY
from s3_tools.aws_utils.sqs import SqsResource


class Commander:
    def __init__(self, lister):
        if not lister or not isinstance(lister, InventoryLister):
            raise ValueError('lister not supported')
        self._lister = lister
        self._sqs = SqsResource(settings)

    def run(self, **kwargs):
        for msg in self._lister.list_objects(**kwargs):
            self._sqs.send_message(msg)


class InventoryLister:

    def __init__(self, source_bucket: str, target_bucket: str, **kwargs):
        super().__init__()
        self._source_bucket = source_bucket
        self._target_bucket = target_bucket

    def list_objects(self, **kwargs):
        """

        :param kwargs:
        :return:
        :rtype: list
        """
        pass


class S3ObjectLister(InventoryLister):

    def __init__(self, source_bucket: str, target_bucket: str, **kwargs):
        super().__init__(source_bucket, target_bucket, **kwargs)
        self._batch_num = settings.get('migration.batch_num', 500)
        self._resource = S3Resource(settings, 'inventory')

    def list_objects(self, **kwargs):
        super().list_objects(**kwargs)
        prefix = kwargs.get('prefix') or None
        owner = kwargs.get('owner') or None
        not_owner = kwargs.get('not_owner') or None
        for keys in self._resource.list_objects_all(bucket=self._source_bucket, prefix=prefix, batch_num=self._batch_num):
            if owner:
                keys = [k for k in keys if k['Owner']['DisplayName'] == owner]
            if not_owner:
                keys = [k for k in keys if k['Owner']['DisplayName'] != not_owner]
            msg = {
                SOURCE_BUCKET_KEY: self._source_bucket,
                TARGET_BUCKET_KEY: self._target_bucket,
                KEYS_KEY: [{
                    SOURCE_KEY_KEY: parse.quote(k['Key']),
                    TARGET_KEY_KEY: parse.quote(k['Key'])
                } for k in keys if not k['Key'].endswith('/')]
            }
            yield msg


class S3InventoryLister(InventoryLister):
    MANIFEST_FILENAME = 'manifest.json'

    def __init__(self, source_bucket: str, target_bucket: str, **kwargs):
        super().__init__(source_bucket, target_bucket, **kwargs)
        self._tmp_dir = settings.get('migration.tmp_dir', 'tmp')
        self._batch_num = settings.get('migration.batch_num', 500)
        self._resource = S3Resource(settings, 'inventory')

    def list_objects(self, **kwargs):
        super().list_objects(**kwargs)
        if 'manifest_path' not in kwargs:
            raise ValueError('manifest_path not provided')
        manifest_path = kwargs.get('manifest_path')
        manifest = self.download_manifest(manifest_path)
        if not self._source_bucket:
            self._source_bucket = manifest.source_bucket
        for f in manifest.files:
            yield from self.process_list_file(f, manifest.dest_bucket)

    def download_manifest(self, manifest_path: str):
        if manifest_path.startswith('s3://'):
            bucket, key = split_s3_path(manifest_path)
            if os.path.basename(key) == '':
                key = key + self.MANIFEST_FILENAME
            elif os.path.basename(key) != self.MANIFEST_FILENAME:
                raise ValueError('Invalid manifest path')
            if not os.path.exists(self._tmp_dir):
                os.makedirs(self._tmp_dir, exist_ok=True)
            local_file = os.path.join(self._tmp_dir, self.MANIFEST_FILENAME)
            logging.info('Download manifest file to {}'.format(local_file))
            self._resource.download_object(bucket=bucket, key=key, filename=local_file)
        else:
            local_file = manifest_path
        return ManifestFile(local_file)

    def process_list_file(self, file_obj, inventory_bucket):
        list_file = self.download_list_file(file_obj, bucket=inventory_bucket)
        if list_file:
            logging.info('Process file {}'.format(list_file))
            with open(list_file) as fp:
                keys = []
                reader = csv.reader(fp)
                for line in reader:
                    key = line[1].strip() if len(line) > 1 else ''
                    if key and not key.endswith('/'):
                        keys.append({SOURCE_KEY_KEY: key, TARGET_KEY_KEY: key})
                        if len(keys) >= self._batch_num:
                            msg = {
                                SOURCE_BUCKET_KEY: self._source_bucket,
                                TARGET_BUCKET_KEY: self._target_bucket,
                                KEYS_KEY: keys
                            }
                            yield msg
                            keys.clear()
                # send last keys
                if len(keys) > 0:
                    msg = {
                        SOURCE_BUCKET_KEY: self._source_bucket,
                        TARGET_BUCKET_KEY: self._target_bucket,
                        KEYS_KEY: keys
                    }
                    yield msg
                    keys.clear()
        else:
            logging.error('Invalid file {}'.format(file_obj['key']))

    def download_list_file(self, file_obj, bucket):
        res = self._resource.list_objects(bucket=bucket, prefix=file_obj['key'], max_keys=1)
        if 'Contents' in res and res['KeyCount'] == 1 \
                and res['Contents'][0]['ETag'].strip('"') == file_obj['MD5checksum']:
            local_file = os.path.join(self._tmp_dir, os.path.basename(file_obj['key']))
            unzip_file = local_file[:-3]
            # download list file
            if not os.path.exists(unzip_file):
                logging.info('Download file {} to {}'.format(file_obj['key'], local_file))
                self._resource.download_object(bucket=bucket, key=file_obj['key'], filename=local_file)
                # extract list file
                logging.info('Extract file {}'.format(os.path.basename(local_file)))
                with open(local_file, 'rb') as fp:
                    g = gzip.GzipFile(mode='rb', fileobj=fp)
                    with open(unzip_file, 'wb') as fout:
                        fout.write(g.read())
                os.unlink(local_file)
            return unzip_file
        else:
            return ''
