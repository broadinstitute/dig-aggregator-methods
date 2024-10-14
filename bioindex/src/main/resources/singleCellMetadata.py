#!/usr/bin/python3
import os
import glob
import gzip
import json
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def download_data():
    out = []
    cmd = 'aws s3 cp s3://dig-analysis-data/single_cell/ ./data/ ' \
          '--recursive --exclude="*" --include="*dataset_metadata.json"'
    subprocess.check_call(cmd, shell=True)
    for file in glob.glob('data/*/dataset_metadata.json'):
        with open(file, 'r') as f:
            out.append(json.load(f))
    return out


def upload_file(data):
    file = 'dataset_metadata.json.gz'
    path = f'{s3_bioindex}/raw/single_cell_metadata/'
    with gzip.open(file, 'wt') as f:
        for d in data:
            f.write(f'{json.dumps(d)}\n')
    subprocess.check_call(['aws', 's3', 'cp', file, path])
    os.remove(file)


def main():
    data = download_data()
    upload_file(data)
    shutil.rmtree('data')


if __name__ == '__main__':
    main()
