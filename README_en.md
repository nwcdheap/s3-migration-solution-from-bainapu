AWS S3 Tools
============

Useful tools for AWS S3.

Migration is useful for large amount of objects to migrate from one bucket to another bucket even cross region or cross account. It supports migration and check between source bucket and target bucket.

# Dependency

Python 3.5+

```
pip install -r requirements.txt
```

# Migration

## Description
Extremely fast S3 migration tool for large amount and small object size buckets. Official `aws s3 cp` command is useful but not efficiency for huge buckets because it is limited by S3 list limitation (http://docs.amazonaws.cn/AmazonS3/latest/dev/request-rate-perf-considerations.html) and not designed for high concurrency. Official `s3-dist-cp` EMR tool (https://docs.aws.amazon.com/emr/latest/ReleaseGuide/UsingEMR_s3distcp.html) is also not so efficiency.

This tool use some methods to accelerate S3 list objects and use SQS to provide high concurrency. It supports several migration modes and supports check and verification during migration.

We have tested migration about 200 million objects (about 2 TB) in one day for 500 executors and about 70 million objects (about 7 TB) in six hours for 1000 executors.

Supported S3 object list methods:

- S3 List Object: use list_objects API
- S3 Inventory: you must enable S3 inventory before do migration


## Configuration

Configuration file is config.yml and a .env file is also supported for environments to overrides config.

Supported Environments:

- INVENTORY_PROFILE_NAME: profile name to get inventory file
- INVENTORY_REGION_NAME: region for inventory file
- INVENTORY_AWS_ACCESS_KEY_ID: access key id to get inventory file
- INVENTORY_AWS_SECRET_ACCESS_KEY: secret access key to get inventory file
- COPY_PROFILE_NAME: profile name to copy objects
- COPY_REGION_NAME: region for copy objects
- COPY_AWS_ACCESS_KEY_ID: access key id to copy objects
- COPY_AWS_SECRET_ACCESS_KEY: secret access key to copy objects
- QUEUE_NAME_PATTERN: queue name pattern
- QUEUE_NUM: queue number
- DEAD_QUEUE_NAME: dead queue name
- MAX_RECEIVE_NUM: max receive message number

Specify another config file use `--config-file` or `-c` option

Specify .env file use `--env-file` or `-e` option

## Usage

### Initialize SQS queues

This tool use several queues to avoid a single queue blocked. You can specify how many queues should be used by `sqs.queue_num` in config.yml.

Create queue and dead-letter queue.

```
python s3_tools.py init
```

### Run commander to list objects to migrate and send messages to SQS.

Use object lister

```
python s3_tools.py commander -s <source_bucket> -t <target_bucket>
```

Use inventory lister

```
python s3_tools.py commander -m s3://<path-to-manifest-file>/ -s <source_bucket> -t <target_bucket>
```

There are two methods to get objects list: ObjectLister and InventoryLister. ObjectLister use list_objects API to get all objects, it is simple and easy to use. InventoryLister use S3 inventory manifest file to list objects. You should enable inventory first and waiting for the file generated. It is fast for large amount of objects.

Parameters:

- --manifest-path, -m: use inventory lister, specify manifest path of S3 inventory, use object lister if not specified
- --source-bucket, -s: source bucket that the objects from, use source_bucket in manifest file if not specified for inventory lister, required for object lister
- --target-bucket, -t: target bucket that the object to
- --batch-num, -b: objects number in one message
- --prefix: prefix for object lister
- --tmp-dir: temp directory for download file
- --owner: send objects if owner match for object lister
- --no-owner: send objects if owner not match for object lister

This command send messages to queues that should be processed by executors.

### Run executor to consume messages and copy objects.

```
python s3_tools.py migration executor
```

Parameters:

- --mode: execute mode, `copy` for directory copy use boto3 copy_object API, `downup` for download object then upload, `check` for object check, send to dead-letter queue if failed
- --queue-num, -n: specify from which queue should receive, default random pick from all queues, `-1` to disable pick and should come with `-d` option
- --including-dead, -d: receive messages including dead-letter queue, use `--queue-num=-1` and `--including-dead` will receive messages only from dead-letter queue
- --verify, -v: add verify for copy/downup mode, it will check objects before copy, it consume more time but is useful for dead-letter queue
- --sleep-sec: sleep seconds if no messages
- --modified-since: copy if object's last modified time after specific time
- --not-modified-since: copy if object's last modified time before specific time
- --tmp-dir: temp directory to store temp files
- --queue-name-pattern: task queue name pattern
- --dead-queue-name: dead-letter queue name
- --max-receive-num: max receive messages number

This command can be run in ECS for high concurrency.

Build docker images by Dockerfile.

Template to create service and task definition can refer to templates/cloudformation.template

# Test

Use pytest to run tests.
```
pytest
```

# Troubleshooting

## Enable S3 inventory

Add inventory configuration in bucket->Management->Inventory tab.
Select ouput format CSV, and better to enable `Size`, `Last modified date`, `Storage class` and `ETag` in optional fields.

This will generate a manifest file and object list data to your specified path.

## Access denied and S3 bucket policy

Because we use the same profile (or key) to get from source bucket and put to target bucket. If the source bucket and target bucket are in different account. You should add bucket policy to the source bucket to allow get object by another account.

Add bucket policy
```
{
    "Version": "2012-10-17",
    "Id": "Policy1535615055992",
    "Statement": [
        {
            "Sid": "allow get",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws-cn:iam::<target bucket account>:root"
            },
            "Action": "s3:*",
            "Resource": "<source bucket arn>"
        },
        {
            "Sid": "allow get",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws-cn:iam::<target bucket account>:root"
            },
            "Action": "s3:Get*",  # s3:GetObject
            "Resource": "<source bucket arn>/*"
        }
    ]
}
```

## Endpoints currently do not support cross-region requests

First solution: Remove S3 endpoints related to your subnet or switch to use EC2s in subnets that do not have S3 endpoints related.

https://docs.aws.amazon.com/vpc/latest/userguide/vpc-endpoints-s3.html

Second solution: The executor use download and upload mode `--mode downup` to download to the EBS and then upload. This requires a big EBS if single object is very large.

## An error occured (SlowDown) when calling the CopyObject operation (reached max retries: 4): Please reduce your request rate.

Concurrency is too high for this bucket and copy rate is limited. Use less then 1000 executors for one bucket may avoid this error.

## An error occurred (InvalidRequest) when calling the CopyObject operation: The specified copy source.

When copy objects larger than 5 GB will cause this error. For objects larger than 5 GB, should use multipart upload API, see https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/RESTObjectCOPY.html . S3 tools will switch these APIs automatically for you.
