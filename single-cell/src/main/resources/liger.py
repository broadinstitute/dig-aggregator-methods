#!/usr/bin/python3
import argparse
import os
import shutil
import subprocess

downloaded_files = '/mnt/var/single_cell'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download(dataset):
    path = f'{s3_in}/single_cell/{dataset}/02072026_scRNA_v3.4.rds'
    cmd = ['aws', 's3', 'cp', path, 'inputs/']
    subprocess.check_call(cmd)


def run_liger():
    try:
        cmd = [
            'Rscript',
            f'{downloaded_files}/inmf_liger_v2-3.R',
            'inputs/02072026_scRNA_v3.4.rds',
            'outputs',
            'study',
            'Cell_Type',
            '5000',
            '50'
        ]
        subprocess.check_call(cmd)
    except:
        pass


def upload(dataset):
    path = f'{s3_out}/out/single_cell/staging/liger/{dataset}/'
    cmd = ['aws', 's3', 'cp', './outputs/', path, '--recursive']
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


if __name__ == '__main__':
    main()
