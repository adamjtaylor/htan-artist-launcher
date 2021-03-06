import sys
import argparse
import boto3
import json

def process_args():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-b', '--bucket', type=str, required=True, help='Bucket name')
    parser.add_argument('-t', '--bucket-type', default='aws', const='aws', nargs='?', choices=['aws', 'gcs'], type=str, help='Bucket name')
    parser.add_argument('-p', '--profile', default='default', type=str, help='AWS profile to use')

    parser.add_argument('-o', '--output-file', type=str)

    args = parser.parse_args()
    return args

def main():
    # Handle input args
    args = process_args()
    bucket_name = args.bucket
    bucket_type = args.bucket_type
    profile = args.profile
    outfile = args.output_file

    # Configure boto3 session and client
    session = boto3.session.Session(profile_name=profile)
    if bucket_type == 'aws':
        s3 = session.resource('s3')
    elif bucket_type == 'gcs':
        # This endpoint is urlencoding spaces into '+' signs prematurely
        s3 = session.resource('s3', endpoint_url='https://storage.googleapis.com')

    # List bucket items
    client = session.client('s3')
    objects = client.list_objects_v2(Bucket='htan-assets')

    print(objects)


if __name__ == '__main__':
    main()
