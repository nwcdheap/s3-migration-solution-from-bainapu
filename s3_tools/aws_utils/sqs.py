"""
Wrapper for boto3 SQS client.

:Author: wuwentao <wuwentao@patsnap.com>

"""
import logging
import json
import time
import random
from hsettings import Settings
from s3_tools.aws_utils import get_aws_session


class SqsResource:

    def __init__(self, settings, profile=None):
        self._settings = settings
        self._profile = profile or 'copy'
        self._client = get_aws_session(self._profile).client('sqs')
        self._queue_url_prefix = ''

    def init_queues(self):
        vt = self.settings.get('sqs.visibility_timeout')
        wt = self.settings.get('sqs.receive_message_wait_time')
        rp = self.settings.get('sqs.message_retention_period')
        # create dead-letter queue
        dead_queue = self.settings.get('sqs.dead_queue_name')
        try:
            logging.info('Create queue {}'.format(dead_queue))
            response = self.client.create_queue(
                QueueName=dead_queue,
                Attributes={
                    'MessageRetentionPeriod': rp,
                    'ReceiveMessageWaitTimeSeconds': wt,
                    'VisibilityTimeout': vt
                }
            )
            queue_url = response['QueueUrl']
            self._queue_url_prefix = get_queue_url_prefix(queue_url)
            response = self.client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['QueueArn']
            )
            dead_queue_arn = response['Attributes']['QueueArn']
        except Exception as e:
            logging.error(e)
            return
        # create queues
        pattern = self.settings.get('sqs.queue_name_pattern')
        num = self.settings.get('sqs.queue_num')
        for i in range(1, num + 1):
            try:
                queue_name = pattern.format(i)
                logging.info('Create queue {}'.format(queue_name))
                response = self.client.create_queue(
                    QueueName=queue_name,
                    Attributes={
                        'MessageRetentionPeriod': rp,
                        'ReceiveMessageWaitTimeSeconds': wt,
                        'VisibilityTimeout': vt,
                        'RedrivePolicy': json.dumps({
                            'deadLetterTargetArn': dead_queue_arn,
                            'maxReceiveCount': '3'
                        })
                    }
                )
            except Exception as e:
                logging.error(e)

    def send_message(self, message: dict, number: int=None, to_dead: bool=False):
        if to_dead:
            queue_name = self.settings.get('sqs.dead_queue_name')
        else:
            queue_name = self.get_queue_name(number)
        queue_url = self.get_queue_url(queue_name)
        logging.info('Send message to queue {}'.format(queue_name))
        response = self.client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message),
        )
        return response

    def receive_message(self, number: int=None, including_dead: bool=False):
        if including_dead:
            if number == -1 or random.randint(0, 1) == 0:
                queue_name = self.settings.get('sqs.dead_queue_name')
            else:
                queue_name = self.get_queue_name(number)
        else:
            queue_name = self.get_queue_name(number)
        queue_url = self.get_queue_url(queue_name)
        max_num = self.settings.get('sqs.max_receive_num')
        response = self.client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_num
        )
        if 'Messages' in response:
            return response['Messages'], queue_url
        return [], queue_url

    def receive_message_loop(self, number: int=None, include_dead: bool=False, sleep_sec: int=5):
        while True:
            messages, queue_url = self.receive_message(number=number, including_dead=include_dead)
            if messages:
                logging.info('receive message: {}'.format(messages))
                for msg in messages:
                    yield msg, queue_url
            else:
                logging.info('no message, sleep for {} seconds'.format(sleep_sec))
                time.sleep(sleep_sec)

    def delete_message(self, queue_url, receipt_handle):
        logging.info('Delete message {}'.format(receipt_handle))
        self.client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

    def get_queue_name(self, number: int=None):
        pattern = self.settings.get('sqs.queue_name_pattern')
        if not number:
            num = self.settings.get('sqs.queue_num')
            number = random.randint(1, num)
        return pattern.format(number)

    def get_queue_url(self, queue_name):
        if not self._queue_url_prefix:
            response = self.client.get_queue_url(QueueName=queue_name)
            queue_url = response['QueueUrl']
            self._queue_url_prefix = get_queue_url_prefix(queue_url)
        else:
            queue_url = self._queue_url_prefix + queue_name
        return queue_url

    @property
    def client(self):
        return self._client

    @property
    def settings(self) -> Settings:
        return self._settings


def get_queue_url_prefix(queue_url):
    return queue_url[:queue_url.rindex('/') + 1]
