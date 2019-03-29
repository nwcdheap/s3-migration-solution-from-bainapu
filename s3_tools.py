#!/usr/bin/python3
import argparse
from s3_tools import load_config
from s3_tools.migration import init_args as migration_args
from s3_tools.logger import init_logger


def main():
    args = init_args()
    settings = load_config(config_file=args.config_file, env_file=args.env_file, obj=vars(args))
    init_logger(log_level=settings['migration.log_level'], log_file=settings['migration.log_file'])
    args.func(vars(args))


def init_args():
    parser = argparse.ArgumentParser(prog='s3_tools')
    subparsers = parser.add_subparsers(help='S3 Tools Commands')
    migration_args(subparsers)
    args = parser.parse_args()
    if not vars(args):
        parser.print_help()
        exit()
    return args


if __name__ == '__main__':
    main()
