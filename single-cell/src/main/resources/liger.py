#!/usr/bin/python3
import argparse
import os
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
        f'{downloaded_files}/inputs/data.h5ad',
        f'{downloaded_files}/outputs',
        'donor_id',
        'cell_type__kp',
        '50000'
    ]
    subprocess.check_call(cmd, cwd=downloaded_files)


def upload(dataset):
    path = f'{s3_out}/out/single_cell/staging/liger/{dataset}/'
    cmd = ['aws', 's3', 'cp', f'{downloaded_files}/outputs/', path, '--recursive']
    subprocess.check_call(cmd)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default=None, required=True, type=str,
                        help="Dataset name")
    args = parser.parse_args()

    import time
    time.sleep(12 * 3600)

    download(args.dataset)
    run_liger()
    upload(args.dataset)


if __name__ == '__main__':
    main()
