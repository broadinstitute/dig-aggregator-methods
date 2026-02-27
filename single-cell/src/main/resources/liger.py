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
    cmd = ['aws', 's3', 'cp', path, f'{downloaded_files}/inputs/{dataset}/']
    subprocess.check_call(cmd)


def run_liger(dataset):
    cmd = [
        'Rscript',
        f'{downloaded_files}/inmf_liger_mod.R',
        f'{downloaded_files}/inputs/{dataset}/data.h5ad',
        f'{downloaded_files}/outputs/{dataset}',
        'donor_id',
        'cell_type__kp',
        '50000'
    ]
    subprocess.check_call(cmd, cwd=downloaded_files)


def upload(dataset):
    shutil.copytree(f'{downloaded_files}/outputs/{dataset}', './outputs')
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
    run_liger(args.dataset)
    upload(args.dataset)


if __name__ == '__main__':
    main()
