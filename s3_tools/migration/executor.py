"""
Executor package for S3 migration.

:Author: wuwentao <wuwentao@patsnap.com>

Executor is used to get migration tasks from SQS queues and execute object migration.
There are three methods now:
- copy: copy objects using boto3 copy or copy_object API, should check permission (bucket policy) first.
- downup: copy objects using download objects and then upload, do not use bucket policy.
"""
import os
import json
import logging
from urllib import parse as urlparse
from queue import Queue
from dateutil.parser import parse
from s3_tools import settings
from s3_tools.migration import SOURCE_BUCKET_KEY, TARGET_BUCKET_KEY, KEYS_KEY, SOURCE_KEY_KEY, TARGET_KEY_KEY
from s3_tools.aws_utils.sqs import SqsResource
from s3_tools.aws_utils.s3 import S3Resource


class Executor:

    def __init__(self, queue_num=None, including_dead=False, mode='copy', verify=False, sleep_sec=5,
                 modified_since=None, not_modified_since=None, **kwargs):
        self._num = queue_num
        self._including_dead = including_dead
        self._mode = mode
        self._verify = verify
        self._sleep_sec = sleep_sec
        self._modified_since = parse(modified_since) if modified_since else None
        self._not_modified_since = parse(not_modified_since) if not_modified_since else None
        self._fails = Queue()
        self._sqs = SqsResource(settings)
        self._s3 = S3Resource(settings)

    def process_message(self, message: dict):
        """
        Process each message from SQS.

        :param dict message:
        :return:
        """
        body = json.loads(message['Body'])
        source_bucket = body[SOURCE_BUCKET_KEY]
        target_bucket = body[TARGET_BUCKET_KEY]
        keys = body[KEYS_KEY]
        logging.info('Receive {} keys from message'.format(len(keys)))
        methods = {
            'copy': self.copy,
            'downup': self.downup,
            'check': self.check
        }
        for key in keys:
            try:
                param = {
                    'source_bucket': source_bucket,
                    'target_bucket': target_bucket,
                    'source_key': urlparse.unquote(key[SOURCE_KEY_KEY]),
                    'target_key': urlparse.unquote(key[TARGET_KEY_KEY])
                }
                method = methods[self._mode]
                method(**param)
            except Exception as e:
                logging.warning(e, exc_info=True)
                logging.warning('Send to dead-letter queue')
                self._fails.put(key)
        self.resend_fails(source_bucket, target_bucket)

    def resend_fails(self, source_bucket: str, target_bucket: str):
        """
        Send fail keys to dead-letter queue.

        :param source_bucket:
        :param target_bucket:
        :return:
        """
        keys = []
        while not self._fails.empty():
            keys.append(self._fails.get())
        if keys:
            msg = {SOURCE_BUCKET_KEY: source_bucket, TARGET_BUCKET_KEY: target_bucket, KEYS_KEY: keys}
            self._sqs.send_message(msg, to_dead=True)

    def copy(self, **kwargs):
        """
        copy mode

        :param kwargs:
        :return:
        """
        if self._verify:
            max_copy_size = 5000000000
            source = self.get_object_info(bucket=kwargs.get('source_bucket'), key=kwargs.get('source_key'))
            target = self.get_object_info(bucket=kwargs.get('target_bucket'), key=kwargs.get('target_key'))
            if not source:
                logging.warning('source object {}/{} not exists!'.format(kwargs.get('source_bucket'), kwargs.get('source_key')))
                return
            size = source['ContentLength']
            if self.verify_object(source, target) and self.verify_metadata(source, target):
                if self.verify_tags(source, target):
                    logging.info('object {}/{} exactly same, skip'
                                 .format(kwargs.get('source_bucket'), kwargs.get('source_key')))
                else:
                    self._s3.put_object_tagging(
                        bucket=kwargs.get('target_bucket'),
                        key=kwargs.get('target_key'),
                        tagging=source['TagSet']
                    )
                    logging.info('object {}/{} copy tags'.format(kwargs.get('source_bucket'), kwargs.get('source_key')))
            else:
                if self.verify_modified(source, self._modified_since, self._not_modified_since):
                    if size < max_copy_size:
                        self._s3.copy_object(**kwargs)
                    else:
                        self._s3.copy(**kwargs)
                    logging.info('copy object {}/{} successfully'
                                 .format(kwargs.get('source_bucket'), kwargs.get('source_key')))
                else:
                    logging.info('object {}/{} mismatch but do not copy because do not pass last modified'
                                 .format(kwargs.get('source_bucket'), kwargs.get('source_key')))
        else:
            source = self.get_object_info(bucket=kwargs.get('source_bucket'), key=kwargs.get('source_key'))
            if not source:
                logging.warning('source object {}/{} not exists!'.format(kwargs.get('source_bucket'), kwargs.get('source_key')))
                return
            if self.verify_modified(source, self._modified_since, self._not_modified_since):
                self._s3.copy(**kwargs)
                logging.info('copy object {}/{} successfully'
                             .format(kwargs.get('source_bucket'), kwargs.get('source_key')))
            else:
                logging.info('object {}/{} do not copy because do not pass last modified'
                             .format(kwargs.get('source_bucket'), kwargs.get('source_key')))

    def download_then_upload(self, source, **kwargs):
        filename = self._s3.download_object(bucket=kwargs.get('source_bucket'), key=kwargs.get('source_key'))
        with open(filename, 'rb') as fp:
            p = {
                'bucket': kwargs.get('target_bucket'),
                'key': kwargs.get('target_key'),
                'body': fp
            }
            if 'CacheControl' in source:
                p['CacheControl'] = source['CacheControl']
            if 'ContentDisposition' in source:
                p['ContentDisposition'] = source['ContentDisposition']
            if 'ContentEncoding' in source:
                p['ContentEncoding'] = source['ContentEncoding']
            if 'ContentLanguage' in source:
                p['ContentLanguage'] = source['ContentLanguage']
            if 'ContentType' in source:
                p['ContentType'] = source['ContentType']
            if 'Metadata' in source and source['Metadata']:
                p['Metadata'] = source['Metadata']
            if 'TagSet' in source and source['TagSet']:
                p['Tagging'] = urlparse.urlencode(dict([(item['Key'], item['Value']) for item in source['TagSet']]))
            self._s3.put_object(**p)
        os.unlink(filename)
        logging.info('download and upload object {}/{} successfully'
                     .format(kwargs.get('source_bucket'), kwargs.get('source_key')))

    def downup(self, **kwargs):
        """
        downup mode

        :param kwargs:
        :return:
        """
        if self._verify:
            source = self.get_object_info(bucket=kwargs.get('source_bucket'), key=kwargs.get('source_key'))
            target = self.get_object_info(bucket=kwargs.get('target_bucket'), key=kwargs.get('target_key'))
            if not source:
                logging.warning(
                    'source object {}/{} not exists!'.format(kwargs.get('source_bucket'), kwargs.get('source_key')))
                return
            if self.verify_object(source, target) and self.verify_metadata(source, target):
                if self.verify_tags(source, target):
                    logging.info('object {}/{} exactly same, skip'
                                 .format(kwargs.get('source_bucket'), kwargs.get('source_key')))
                else:
                    self._s3.put_object_tagging(
                        bucket=kwargs.get('target_bucket'),
                        key=kwargs.get('target_key'),
                        tagging=source['TagSet']
                    )
                    logging.info('object {}/{} copy tags'.format(kwargs.get('source_bucket'), kwargs.get('source_key')))
            else:
                if self.verify_modified(source, self._modified_since, self._not_modified_since):
                    self.download_then_upload(source, **kwargs)
                else:
                    logging.info('object {}/{} mismatch but do not copy because do not pass last modified'
                                 .format(kwargs.get('source_bucket'), kwargs.get('source_key')))
        else:
            source = self.get_object_info(bucket=kwargs.get('source_bucket'), key=kwargs.get('source_key'))
            if not source:
                logging.warning(
                    'source object {}/{} not exists!'.format(kwargs.get('source_bucket'), kwargs.get('source_key')))
                return
            if self.verify_modified(source, self._modified_since, self._not_modified_since):
                self.download_then_upload(source, **kwargs)
            else:
                logging.info('object {}/{} do not copy because do not pass last modified'
                             .format(kwargs.get('source_bucket'), kwargs.get('source_key')))

    def check(self, **kwargs):
        """
        check mode

        :param kwargs:
        :return:
        """
        source = self.get_object_info(bucket=kwargs.get('source_bucket'), key=kwargs.get('source_key'))
        target = self.get_object_info(bucket=kwargs.get('target_bucket'), key=kwargs.get('target_key'))
        if not source:
            logging.warning(
                'source object {}/{} not exists!'.format(kwargs.get('source_bucket'), kwargs.get('source_key')))
            return True
        if self.verify_object(source, target) and self.verify_metadata(source, target) and self.verify_tags(source, target):
            return True
        raise ValueError('object {}/{} mismatch'.format(kwargs.get('source_bucket'), kwargs.get('source_key')))

    def verify_object(self, source_obj, target_obj) -> bool:
        if source_obj and target_obj and source_obj['ETag'] == target_obj['ETag']:
            return True
        return False

    def verify_metadata(self, source_obj, target_obj) -> bool:
        fields = ['CacheControl', 'ContentDisposition', 'ContentEncoding', 'ContentLanguage', 'ContentType']
        p1 = dict([(k, source_obj[k]) for k in fields if k in source_obj])
        p2 = dict([(k, target_obj[k]) for k in fields if k in target_obj])
        if source_obj and target_obj and source_obj['Metadata'] == target_obj['Metadata'] and p1 == p2:
            return True
        return False

    def verify_tags(self, source_obj, target_obj) -> bool:
        if source_obj and target_obj and source_obj['TagSet'] == target_obj['TagSet']:
            return True
        return False

    def verify_modified(self, source_obj, modified_since=None, not_modified_since=None) -> bool:
        if modified_since:
            last_modified = source_obj['LastModified'].replace(tzinfo=None)
            if last_modified >= modified_since:
                return True
            return False
        if not_modified_since:
            last_modified = source_obj['LastModified'].replace(tzinfo=None)
            if last_modified < not_modified_since:
                return True
            return False
        return True

    def get_object_info(self, bucket: str, key: str):
        """

        :param bucket:
        :param key:
        :return: object info
        :rtype: dict
        """
        try:
            obj = self._s3.head_object(bucket=bucket, key=key)
            tags = self._s3.get_object_tagging(bucket=bucket, key=key)
            obj['TagSet'] = tags['TagSet']
            return obj
        except Exception as e:
            return None

    def run(self):
        for message, queue_url in self._sqs.receive_message_loop(number=self._num, include_dead=self._including_dead,
                                                                 sleep_sec=self._sleep_sec):
            try:
                self.process_message(message)
                self._sqs.delete_message(queue_url=queue_url, receipt_handle=message['ReceiptHandle'])
            except Exception as e:
                logging.error(e, exc_info=True)
