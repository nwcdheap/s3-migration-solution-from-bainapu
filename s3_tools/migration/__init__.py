import logging


SOURCE_BUCKET_KEY = 'source_bucket'
TARGET_BUCKET_KEY = 'target_bucket'
KEYS_KEY = 'keys'
SOURCE_KEY_KEY = 'source_key'
TARGET_KEY_KEY = 'target_key'


def common_init_args(parser):
    parser.add_argument('--log-level', help='logger level', default='INFO')
    parser.add_argument('--log-file', help='logger file')
    parser.add_argument('-c', '--config-file', help='config file path, default config.yml', default='config.yml')
    parser.add_argument('-e', '--env-file', help='env file path')


def commander_init_args(parser):
    parser.add_argument('-m', '--manifest-path',
                        help='manifest file path for S3InventoryLister, use S3InventoryLister if specified')
    parser.add_argument('-s', '--source-bucket',
                        help='source bucket for commander, or use sourceBucket in manifest for S3InventoryLister')
    parser.add_argument('-t', '--target-bucket', help='target bucket for commander', required=True)
    parser.add_argument('-p', '--prefix', help='prefix for objects, only used for S3ObjectLister')
    parser.add_argument('-b', '--batch-num', help='objects number in one message', type=int)
    parser.add_argument('--tmp-dir', help='temp directory to store temp files')
    parser.add_argument('--owner', help='send if owner match for S3ObjectLister')
    parser.add_argument('--no-owner', help='send if owner not match for S3ObjectLister')
    common_init_args(parser)
    parser.set_defaults(func=run_commander)


def executor_init_args(parser):
    parser.add_argument('-v', '--verify', help='verify object before migration', action='store_true')
    parser.add_argument('-n', '--queue-num', help='specify queue number, specify -1 to use none, default random pick',
                        type=int)
    parser.add_argument('-d', '--including-dead', help='receive message including dead-letter queue for executor',
                        action='store_true')
    parser.add_argument('--mode', help='executor mode, directory copy object or download then upload or just check',
                        choices=['copy', 'downup', 'check'], default='copy')
    parser.add_argument('--sleep-sec', help='sleep seconds if no messages', default=5, type=int)
    parser.add_argument('--modified-since', help='copy if object\'s last modified time after specific time')
    parser.add_argument('--not-modified-since', help='copy if object\'s last modified time before specific time')
    parser.add_argument('--tmp-dir', help='temp directory to store temp files')
    parser.add_argument('--queue-name-pattern', help='task queue name pattern')
    parser.add_argument('--dead-queue-name', help='dead-letter queue name')
    parser.add_argument('--max-receive-num', help='max receive messages number', type=int)
    common_init_args(parser)
    parser.set_defaults(func=run_executor)


def initializer_init_args(parser):
    common_init_args(parser)
    parser.set_defaults(func=run_init)


def run_commander(args):
    from s3_tools.migration.commander import Commander
    if args['manifest_path']:
        # use inventory manifest file
        from s3_tools.migration.commander import S3InventoryLister
        logging.info('Use inventory manifest lister')
        lister = S3InventoryLister(**args)
    else:
        # default use list_objects API
        from s3_tools.migration.commander import S3ObjectLister
        logging.info('Use list_objects API lister')
        lister = S3ObjectLister(**args)
    comd = Commander(lister=lister)
    comd.run(**args)


def run_executor(args):
    from s3_tools.migration.executor import Executor
    p = parse_args(args)
    exe = Executor(**p)
    exe.run()


def run_init(args):
    from s3_tools.aws_utils.sqs import SqsResource
    from s3_tools import settings
    sqs = SqsResource(settings)
    sqs.init_queues()


def init_args(subparsers):
    parser = subparsers.add_parser('commander', help='S3 Migration Commander')
    commander_init_args(parser)
    parser = subparsers.add_parser('executor', help='S3 Migration Executor')
    executor_init_args(parser)
    parser = subparsers.add_parser('init', help='Initialize Migration Queues')
    initializer_init_args(parser)


def parse_args(args: dict) -> dict:
    common_args = ['log_level', 'log_file', 'config_file', 'env_file']
    return dict([(k, v) for k, v in args.items() if k not in common_args])
