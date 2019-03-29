from hsettings import Settings


__prog__ = 's3_tools'
__version__ = '1.2.2'
__author__ = 'wuwentao <wuwentao@patsnap.com>'


settings = Settings()


def load_config_file(config_file):
    from hsettings.loaders import YamlLoader
    conf = YamlLoader.load(config_file)
    settings.merge(conf)


def load_env(env_file):
    from hsettings.loaders import EnvLoader
    env_mappings = {
        'INVENTORY_PROFILE_NAME': 'aws.inventory.profile_name',
        'INVENTORY_REGION_NAME': 'aws.inventory.region_name',
        'INVENTORY_AWS_ACCESS_KEY_ID': 'aws.inventory.aws_access_key_id',
        'INVENTORY_AWS_SECRET_ACCESS_KEY': 'aws.inventory.aws_secret_access_key',
        'COPY_PROFILE_NAME': 'aws.copy.profile_name',
        'COPY_REGION_NAME': 'aws.copy.region_name',
        'COPY_AWS_ACCESS_KEY_ID': 'aws.copy.aws_access_key_id',
        'COPY_AWS_SECRET_ACCESS_KEY': 'aws.copy.aws_secret_access_key',
        'COPY_SOURCE_PROFILE_NAME': 'aws.copy_source.profile_name',
        'COPY_SOURCE_REGION_NAME': 'aws.copy_source.region_name',
        'COPY_SOURCE_AWS_ACCESS_KEY_ID': 'aws.copy_source.aws_access_key_id',
        'COPY_SOURCE_AWS_SECRET_ACCESS_KEY': 'aws.copy_source.aws_secret_access_key',
        'QUEUE_NAME_PATTERN': 'sqs.queue_name_pattern',
        'QUEUE_NUM': 'sqs.queue_num',
        'DEAD_QUEUE_NAME': 'sqs.dead_queue_name',
        'MAX_RECEIVE_NUM': 'sqs.max_receive_num',
    }
    casts = {
        'QUEUE_NUM': int,
        'BATCH_NUM': int,
        'MAX_RECEIVE_NUM': int
    }
    conf = EnvLoader.load(env_file, env_to_key_mapping=env_mappings, casts=casts, only_key_mappings_includes=True)
    settings.merge(conf)


def load_dict(obj):
    from hsettings.loaders import DictLoader
    mappings = {
        'log_level': 'migration.log_level',
        'log_file': 'migration.log_file',
        'queue_name_pattern': 'sqs.queue_name_pattern',
        'dead_queue_name': 'sqs.dead_queue_name',
        'max_receive_num': 'sqs.max_receive_num',
        'batch_num': 'migration.batch_num',
        'tmp_dir': 'migration.tmp_dir',
    }
    casts = {}
    obj = dict([(k, v) for k, v in obj.items() if v])
    conf = DictLoader.load(obj, casts=casts, key_mappings=mappings, only_key_mappings_includes=True)
    settings.merge(conf)


def load_config(config_file, env_file, obj):
    if config_file:
        load_config_file(config_file)
    load_env(env_file)
    if obj:
        load_dict(obj)
    return settings
