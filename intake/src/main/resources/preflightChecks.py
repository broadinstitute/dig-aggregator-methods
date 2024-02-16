#!/usr/bin/python3
import argparse
import boto3
from gzip import GzipFile
from io import TextIOWrapper
import json
import os
from PortalDB import PortalDB
import re
import subprocess


input_path = os.environ['INPUT_PATH']

portal_to_g1000_ancestry = {
    'AA': 'AFR',
    'AF': 'AFR',
    'SSAF': 'AFR',
    'EA': 'EAS',
    'EU': 'EUR',
    'HS': 'AMR',
    'SA': 'SAS',
    'GME': 'SAS',
    'Mixed': 'EUR'
}

required_col_names = ['chromosome', 'position', 'reference', 'alt', 'pValue',
                      'oddsRatio', 'beta', 'stdErr', 'eaf', 'n']


def get_bucket_key(path):
    return f'{input_path}/variants_raw/{path}/metadata'.split('/', 3)[2:]


def check_dichotomous_information(exceptions, db, phenotype):
    try:
        db.get_is_dichotomous(phenotype)
    except Exception:
        exceptions.append(f'ERROR: {phenotype} not in Portal DB or missing dichotomous information')


def check_dataset_data(exceptions, db, dataset):
    try:
        data = db.get_dataset_data(dataset)
        if data['ancestry'] not in portal_to_g1000_ancestry:
            print(f"ERROR: Ancestry not mapped to g1000 - {dataset} with ancestry {data['ancestry']}")
    except Exception:
        exceptions.append(f'ERROR: {dataset} not in Portal DB or missing ancestry information')


def get_metadata(db, s3_client, path):
    bucket, key = get_bucket_key(path)
    content_object = s3_client.get_object(Bucket=bucket, Key=key)
    metadata = json.loads(content_object['Body'].read().decode())
    metadata['dichotomous'] = db.get_is_dichotomous(metadata['phenotype'])
    dataset_data = db.get_dataset_data(metadata['dataset'])
    metadata['ancestry'] = dataset_data['ancestry']
    return metadata


def get_file_key(s3_client, path):
    bucket, key = get_bucket_key(path)
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
    for content in response['Contents']:
        if '.gz' in content['Key']:
            return bucket, content['Key']


def get_header_field_list(s3_client, path):
    bucket, key = get_file_key(s3_client, path)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    gzipped = GzipFile(None, 'rb', fileobj=response['Body'])
    return re.split(r'[\t,]+', TextIOWrapper(gzipped).readline().strip())


def check_degeneracy(exceptions, path, metadata_values):
    if len(metadata_values) != len(set(metadata_values)):
        exceptions.append(f'degenerate column values for {path}')


def check_required_fields(exceptions, metadata, path):
    for c in required_col_names:
        if c not in metadata['column_map']:
            exceptions.append(f'{c} not in metadata for {path}')


def match_col_names(exceptions, metadata, header_field_list):
    for k, v in metadata['column_map'].items():
        if v is not None:
            if v not in header_field_list:
                header_str = ','.join(header_field_list)
                exceptions.append(f'{v} is not in file header {header_str}')


def check_n_inference(exceptions, metadata):
    if metadata['column_map']['n'] is None:
        if metadata['dichotomous'] and (metadata['cases'] is None or metadata['controls'] is None):
            exceptions.append(f'Cannot infer n for dichotomous trait')
        if not metadata['dichotomous'] and metadata['subjects'] is None:
            exceptions.append(f'Cannot infer n for non-dichotomous trait')


def check_metadata(exceptions, db, s3_client, path):
    try:
        metadata = get_metadata(db, s3_client, path)
        header_field_list = get_header_field_list(s3_client, path)

        metadata_values = [v for v in metadata['column_map'].values() if v is not None]
        check_degeneracy(exceptions, path, metadata_values)
        match_col_names(exceptions, metadata, header_field_list)
        check_n_inference(exceptions, metadata)
    except Exception:
        exceptions.append(f'ERROR: file / metadata for {path} invalid')


def mark_path_success(path):
    s3_path = f'{input_path}/variants_raw/{path}/'
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', s3_path])
    os.remove('_SUCCESS')


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--filepath', type=str, required=True)
    args = opts.parse_args()

    db = PortalDB()
    s3_client = boto3.client('s3')

    exceptions = []
    tech, dataset, phenotype = args.path.split('/')
    check_dichotomous_information(exceptions, db, phenotype)
    check_dataset_data(exceptions, db, dataset)
    check_metadata(exceptions, db, s3_client, args.path)
    for exception in exceptions:
        print(exception)
    if len(exceptions) == 0:
        mark_path_success(args.path)


if __name__ == '__main__':
    main()
