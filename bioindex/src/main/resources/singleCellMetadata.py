#!/usr/bin/python3
import os
import glob
import gzip
import json
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def add_data(out, path, data_type):
    cmd = f'aws s3 cp {s3_in}/{path}/ ./data/ ' \
          '--recursive --exclude="*" --include="*dataset_metadata.json"'
    subprocess.check_call(cmd, shell=True)
    for file in glob.glob('data/*/dataset_metadata.json'):
        with open(file, 'r') as f:
            json_data = json.load(f)
            json_data['data_type'] = data_type
            out.append(json_data)
    shutil.rmtree('data')
    return out


def upload_file(data):
    file = 'dataset_metadata.json.gz'
    path = f'{s3_bioindex}/raw/single_cell_all_metadata/'
    with gzip.open(file, 'wt') as f:
        for d in data:
            f.write(f'{json.dumps(d)}\n')
    subprocess.check_call(['aws', 's3', 'cp', file, path])
    os.remove(file)


def main():
    data = []
    data = add_data(data, 'single_cell', 'single_cell')
    data = add_data(data, 'bulk_rna', 'bulk_rna')
    upload_file(data)


if __name__ == '__main__':
    main()
