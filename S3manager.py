#!/usr/bin/python3
"""
Simple S3 manager
:created: 04-19-2016
:updated: 04-21-2016
:author: Jacopo Scrinzi <scrinzi.jacopo@gmail.com>
"""
import configparser
import boto3
import sys
import os
import argparse


def get_s3_client(credential):
    """
    Get S3 boto3 client

    :param credential: configparser.ConfigParser
    :return: boto3.client
    """
    if "~" in credential:
        credential = credential.replace("~", os.path.expanduser("~"))
    config = configparser.ConfigParser()
    read = config.read(credential)
    if len(read) == 0:
        raise Exception("ERROR: AWS credential file not found in {}".format(
            credential))
    if "default" not in config.sections():
        raise Exception("ERROR: [default] section not found in {}".format(
            credential))
    default = dict(config["default"])
    return boto3.client("s3", **default)


def bucket_exists(bucket_name, s3_client):
    """
    Check if bucket exists on Amazon s3

    :param bucket_name: str
    :param s3_client: boto3.clint
    :return: bool
    """
    buckets = s3_client.list_buckets()
    for bucket in buckets["Buckets"]:
        if bucket["Name"] == bucket_name:
            return True
    return False


def bucket_content(bucket_name, s3_client):
    """
    Get S3 bucket content
    """
    bucket = s3_client.list_objects(Bucket=bucket_name)
    content = []
    if "Contents" not in bucket:
        return content
    for key in bucket["Contents"]:
        content.append(key["Key"])
    return content


def file_check(filename, bucket_name, s3_client):
    """
    Check file exists in S3 bucket

    :param filename: str
    :param bucket_name: str
    :param s3_client: boto3.clint
    :return: bool
    """
    content = bucket_content(bucket_name, s3_client)
    if filename not in content:
        return False
    return True


def upload_file(s3_client, bucket_name, bucket_file, host_file, force=False):
    """
    Upload File to Amazon S3

    :param s3_client: boto3.client
    :param bucket_name: str
    :param bucket_file: str
    :param host_file: str
    :param force: bool
    :return: bool
    """
    if not bucket_exists(bucket_name, s3_client):
        s3_client.create_bucket(Bucket=bucket_name)
        if not bucket_exists(bucket_name, s3_client):
            sys.stderr.write("ERROR: Unable to create bucket: {}\n".format(
                bucket_name))
            return False
    if not os.path.isfile(host_file):
        sys.stderr.write("ERROR: {} not found\n".format(host_file))
        return False
    if file_check(bucket_file, bucket_name, s3_client):
        if force is not True:
            sys.stderr.write("ERROR: {} already present\n".format(bucket_file))
            return False
    s3_client.upload_file(host_file, bucket_name, bucket_file)
    if not file_check(bucket_file, bucket_name, s3_client):
        sys.stderr.write("ERROR: Unable to upload {}\n".format(host_file))
        return False
    sys.stdout.write("File: {} uploaded successfully to {} in bucket\n".
                     format(host_file, bucket_file, bucket_name))
    return True


def download_file(s3_client, bucket_name, bucket_file, host_file, force=False):
    """
    Download file from Amazon S3

    :param s3_client: boto3.client
    :param bucket_name: str
    :param bucket_file: str
    :param host_file: str
    :param force: bool
    :return: bool
    """
    dst_dir = "/".join(host_file.split("/")[:-1])
    if dst_dir:
        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)
    if not file_check(bucket_file, bucket_name, s3_client):
        sys.stderr.write("ERROR: {} not found in bucket: {}\n".format(
            bucket_file, bucket_name))
        return False
    if os.path.isfile(host_file):
        if force is not True:
            sys.stderr.write("ERROR: {} already exists\n".format(host_file))
            return False
    s3_client.download_file(bucket_name, bucket_file, host_file)
    if not os.path.isfile(host_file):
        sys.stderr.write(
            "ERROR: Unable to download {} to {} from bucket: {}\n".format(
                bucket_file, host_file, bucket_name))
        return False
    sys.stdout.write("File:{} downloaded successfully to {}\n".
                     format(bucket_file, host_file))
    return True


def delete_file(s3_client, bucket_name, bucket_file):
    """
    Delete file from S3 bucket

    :param s3_client: boto3.client
    :param bucket_name: str
    :param bucket_file: str
    :return: bool
    """
    if not bucket_exists(bucket_name, s3_client):
        sys.stderr.write("ERROR: Unable to find bucket {}\n".
                         format(bucket_name))
        return False
    if not file_check(bucket_file, bucket_name, s3_client):
        sys.stderr.write("ERROR: Unable to find {} in bucket {}\n".
                         format(bucket_file, bucket_name))
        return False
    s3_client.delete_object(Bucket=bucket_name, Key=bucket_file)
    if file_check(bucket_file, bucket_name, s3_client):
        sys.stderr.write("ERROR: Unable to delete {} in bucket {}\n".
                         format(bucket_file, bucket_name))
        return False
    sys.stdout.write("File: {} deleted successfully\n".format(bucket_file))
    return True


def delete_all_files(s3_client, bucket_name):
    """
    Delete all files in S3 Bucket

    :param s3_client: boto3.client
    :param bucket_name: str
    :return: bool
    """
    content = bucket_content(bucket_name, s3_client)
    for bucket_file in content:
        delete_file(s3_client, bucket_name, bucket_file)


def delete_bucket(s3_client, bucket_name, force=False):
    """
    Delete S3 Bucket

    :param s3_client: boto3.client
    :param bucket_name: str
    :param force: bool
    :return: bool
    """
    if not bucket_exists(bucket_name, s3_client):
        sys.stdout.write("ERROR: {} does not exists\n".format(bucket_name))
        return False
    content = bucket_content(bucket_name, s3_client)
    if content:
        if force is not True:
            sys.stdout.write("ERROR: {} not empty\n".format(bucket_name))
            return False
        delete_all_files(s3_client, bucket_name)
    s3_client.delete_bucket(Bucket=bucket_name)
    if bucket_exists(bucket_name, s3_client):
        sys.stderr.write("ERROR: unable to delete bucket {}\n".
                         format(bucket_name))
        return False
    sys.stdout.write("Bucket:{} deleted successfully\n".format(bucket_name))
    return True


def exit_code(arg):
    """
    Exit with the right exit code depending on the arg passed

    :param arg: bool
    :return: void
    """
    if arg is True:
        sys.exit(0)
    sys.exit(1)


def main(parser):
    """
    Main function

    :param parser: argparse
    """
    args = parser.parse_args()
    try:
        s3 = get_s3_client(args.credential)
    except Exception as e:
        sys.stderr.write("{}\n".format(str(e)))
        sys.exit(1)
    if args.subparser_name == "download":
        exit_code(download_file(
            s3, args.bucket_name, args.bucket_file, args.host_file, args.f))
    elif args.subparser_name == "upload":
        exit_code(upload_file(
            s3, args.bucket_name, args.bucket_file, args.host_file, args.f))
    elif args.subparser_name == "delete-file":
        exit_code(delete_file(s3, args.bucket_name, args.bucket_file))
    elif args.subparser_name == "delete-all-files":
        exit_code(delete_all_files(s3, args.bucket_name))
    elif args.subparser_name == "delete-bucket":
        exit_code(delete_bucket(s3, args.bucket_name, args.f))
    else:
        parser.print_help()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="S3 Manager", description="Amazon S3 manager")
    subparsers = parser.add_subparsers(
        dest="subparser_name", help="sub-command help")
    parser_download = subparsers.add_parser(
        "download", help="Download file from S3 bucket")
    parser_upload = subparsers.add_parser(
        "upload", help="Upload file to S3 bucket")
    parser_delete_file = subparsers.add_parser(
        "delete-file", help="Delete file in S3 bucket")
    parser_delete_allfiles = subparsers.add_parser(
        "delete-all-files", help="Delete all files in S3 bucket")
    parser_delete_bucket = subparsers.add_parser(
        "delete-bucket", help="Delete S3 bucket")
    parser_download.add_argument("-f", action='store_true', help="Force")
    parser_download.add_argument("bucket_name", help="S3 bucket name")
    parser_download.add_argument(
        "bucket_file", help="Full path file in Bucket")
    parser_download.add_argument("host_file", help="Full path file on host")
    parser_upload.add_argument("-f", action='store_true', help="Force")
    parser_upload.add_argument("bucket_name", help="S3 bucket name")
    parser_upload.add_argument("bucket_file", help="Full path file in Bucket")
    parser_upload.add_argument("host_file", help="Full path file on host")
    parser_delete_file.add_argument("bucket_name", help="S3 bucket name")
    parser_delete_file.add_argument(
        "bucket_file", help="Full path file in Bucket")
    parser_delete_allfiles.add_argument("bucket_name", help="S3 bucket name")
    parser_delete_bucket.add_argument("-f", action='store_true', help="Force")
    parser_delete_bucket.add_argument("bucket_name", help="S3 bucket name")
    parser.add_argument(
        "-c", "--credential", default="~/.aws/credentials",
        help="Path AWS credential file")
    main(parser)
