import os
import json
import pytest
from s3_tools import load_config


settings = load_config(os.path.join(os.path.dirname(__file__), '../config.yml'), None, None)


class TestExecutor:

    source_profile = 'inventory'
    target_profile = 'copy'
    source_bucket = ''
    key_prefix = 'test'
    target_bucket = ''

    def test_veirfy(self):
        from s3_tools.migration.executor import Executor
        from s3_tools.aws_utils.s3 import S3Resource
        source_key = self.key_prefix + '/test.txt'
        target_key = source_key
        executor = Executor(mode='check')
        s31 = S3Resource(settings, self.source_profile)
        s32 = S3Resource(settings, self.target_profile)
        self.upload_test_file(client=s31, bucket=self.source_bucket, key=source_key)
        self.upload_test_file(client=s32, bucket=self.target_bucket, key=target_key)
        source = executor.get_object_info(bucket=self.source_bucket, key=source_key)
        target = executor.get_object_info(bucket=self.target_bucket, key=target_key)
        assert executor.verify_object(source, target) is True
        assert executor.verify_metadata(source, target) is True
        assert executor.verify_tags(source, target) is True
        self.upload_test_file(client=s32, bucket=self.target_bucket, key=target_key, body='s3_tools2'.encode('utf-8'))
        source = executor.get_object_info(bucket=self.source_bucket, key=source_key)
        target = executor.get_object_info(bucket=self.target_bucket, key=target_key)
        assert executor.verify_object(source, target) is False
        assert executor.verify_metadata(source, target) is True
        assert executor.verify_tags(source, target) is True
        self.upload_test_file(client=s32, bucket=self.target_bucket, key=target_key, ContentType='plain/text')
        source = executor.get_object_info(bucket=self.source_bucket, key=source_key)
        target = executor.get_object_info(bucket=self.target_bucket, key=target_key)
        assert executor.verify_object(source, target) is True
        assert executor.verify_metadata(source, target) is False
        assert executor.verify_tags(source, target) is True
        self.upload_test_file(client=s32, bucket=self.target_bucket, key=target_key, Tagging='tag1=aaa')
        source = executor.get_object_info(bucket=self.source_bucket, key=source_key)
        target = executor.get_object_info(bucket=self.target_bucket, key=target_key)
        assert executor.verify_object(source, target) is True
        assert executor.verify_metadata(source, target) is True
        assert executor.verify_tags(source, target) is False

    def test_copy_mode(self):
        from s3_tools.migration.executor import Executor
        executor = Executor(mode='copy')
        self.process_executor_message(executor)

    def test_downup_mode(self):
        from s3_tools.migration.executor import Executor
        executor = Executor(mode='downup')
        self.process_executor_message(executor)

    def test_check_mode(self):
        from s3_tools.migration.executor import Executor
        from s3_tools.aws_utils.s3 import S3Resource
        source_key = self.key_prefix + '/test.txt'
        target_key = source_key
        executor = Executor(mode='check')
        s31 = S3Resource(settings, self.source_profile)
        s32 = S3Resource(settings, self.target_profile)
        self.upload_test_file(client=s31, bucket=self.source_bucket, key=source_key)
        with pytest.raises(ValueError):
            res = executor.check(
                source_bucket=self.source_bucket,
                source_key=source_key,
                target_bucket=self.target_bucket,
                target_key=target_key
            )
        self.upload_test_file(client=s32, bucket=self.target_bucket, key=target_key)
        res = executor.check(
            source_bucket=self.source_bucket,
            source_key=source_key,
            target_bucket=self.target_bucket,
            target_key=target_key
        )
        assert res is True
        s31.delete_object(bucket=self.source_bucket, key=source_key)
        s32.delete_object(bucket=self.target_bucket, key=target_key)

    def test_copy_verify_mode(self):
        from s3_tools.migration.executor import Executor
        from s3_tools.aws_utils.s3 import S3Resource
        source_key = self.key_prefix + '/test.txt'
        target_key = source_key
        s31 = S3Resource(settings, self.source_profile)
        s32 = S3Resource(settings, self.target_profile)
        self.upload_test_file(client=s31, bucket=self.source_bucket, key=source_key)
        self.upload_test_file(client=s32, bucket=self.target_bucket, key=target_key, CacheControl='aaaa')
        executor = Executor(mode='copy', verify=True)
        executor.copy(
            source_bucket=self.source_bucket,
            source_key=source_key,
            target_bucket=self.target_bucket,
            target_key=target_key
        )
        self.check_objects(
            source_client=s31,
            source_bucket=self.source_bucket,
            source_key=source_key,
            target_client=s32,
            target_bucket=self.target_bucket,
            target_key=target_key
        )
        s31.delete_object(bucket=self.source_bucket, key=source_key)
        s32.delete_object(bucket=self.target_bucket, key=target_key)

    def upload_test_file(self, client, bucket, key, **kwargs):
        param = {
            'bucket': bucket,
            'key': key,
            'body': 's3_tools'.encode('utf-8'),
            'ContentType': 'application/json',
            'Metadata': {
                'test1': '111'
            },
            'Tagging': 'tag1=val1&tag2=val2'
        }
        if kwargs:
            param.update(kwargs)
        client.put_object(**param)

    def process_executor_message(self, executor):
        from s3_tools.aws_utils.s3 import S3Resource
        s31 = S3Resource(settings, self.source_profile)
        s32 = S3Resource(settings, self.target_profile)
        source_key = self.key_prefix + '/test.txt'
        target_key = source_key
        self.upload_test_file(
            client=s31,
            bucket=self.source_bucket,
            key=source_key
        )
        executor.process_message(
            message={'Body': json.dumps({
                'source_bucket': self.source_bucket,
                'target_bucket': self.target_bucket,
                'keys': [{'source_key': source_key, 'target_key': target_key}]
            })}
        )
        self.check_objects(
            source_client=s31,
            source_bucket=self.source_bucket,
            source_key=source_key,
            target_client=s32,
            target_bucket=self.target_bucket,
            target_key=target_key
        )
        s31.delete_object(bucket=self.source_bucket, key=source_key)
        s32.delete_object(bucket=self.target_bucket, key=target_key)

    def check_objects(self, source_client, source_bucket, source_key, target_client, target_bucket, target_key):
        res1 = source_client.head_object(bucket=source_bucket, key=source_key)
        res2 = target_client.head_object(bucket=target_bucket, key=target_key)
        assert res1['ETag'] == res2['ETag']
        assert res1['ContentLength'] == res2['ContentLength']
        assert res1['Metadata'] == res2['Metadata']
        assert ('CacheControl' in res1) == ('CacheControl' in res2)
        if 'CacheControl' in res1:
            assert res1['CacheControl'] == res2['CacheControl']
        assert ('ContentDisposition' in res1) == ('ContentDisposition' in res2)
        if 'ContentDisposition' in res1:
            assert res1['ContentDisposition'] == res2['ContentDisposition']
        assert ('ContentEncoding' in res1) == ('ContentEncoding' in res2)
        if 'ContentEncoding' in res1:
            assert res1['ContentEncoding'] == res2['ContentEncoding']
        assert ('ContentLanguage' in res1) == ('ContentLanguage' in res2)
        if 'ContentLanguage' in res1:
            assert res1['ContentLanguage'] == res2['ContentLanguage']
        assert ('ContentType' in res1) == ('ContentType' in res2)
        if 'ContentType' in res1:
            assert res1['ContentType'] == res2['ContentType']
        res1 = source_client.get_object_tagging(bucket=source_bucket, key=source_key)
        res2 = target_client.get_object_tagging(bucket=target_bucket, key=target_key)
        assert res1['TagSet'] == res2['TagSet']
