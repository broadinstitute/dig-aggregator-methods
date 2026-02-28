#!/usr/bin/python3
import argparse
import os
import shutil
import subprocess

downloaded_files = '/mnt/var/single_cell'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download(dataset):
    path = f'{s3_in}/single_cell/{dataset}/data.h5ad'
    cmd = ['aws', 's3', 'cp', path, 'inputs/']
    subprocess.check_call(cmd)


def run_liger():
    cmd = [
        'Rscript',
        f'{downloaded_files}/inmf_liger_mod.R',
        'inputs/data.h5ad',
        'outputs',
        'donor_id',
        'cell_type__kp',
        '50000'
    ]
    subprocess.check_call(cmd)


def upload(dataset):
    zip_cmd = ['zip', '-r', 'liger.zip', './outputs/']
    subprocess.check_call(zip_cmd)
    path = f'{s3_out}/out/single_cell/staging/liger/{dataset}/'
    cmd = ['aws', 's3', 'cp', 'liger.zip', path]
    subprocess.check_call(cmd)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default=None, required=True, type=str,
                        help="Dataset name")
    args = parser.parse_args()

    download(args.dataset)
    run_liger()
    upload(args.dataset)

    shutil.rmtree('inputs')
    shutil.rmtree('outputs')
    os.remove('liger.zip')


if __name__ == '__main__':
    main()
