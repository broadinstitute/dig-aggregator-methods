#!/usr/bin/python3
import argparse
import os
import shutil
import subprocess

downloaded_files = '/mnt/var/single_cell'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download(dataset, cell_type):
    path = f'{s3_in}/out/single_cell/staging/h5ad/{dataset}/{cell_type}.h5ad'
    cmd = ['aws', 's3', 'cp', path, 'inputs/data.h5ad']
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
    path = f'{s3_out}/out/single_cell/staging/liger/{dataset}/'
    cmd = ['aws', 's3', 'cp', './outputs/', path, '--recursive']
    subprocess.check_call(cmd)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default=None, required=True, type=str,
                        help="Dataset name")
    parser.add_argument('--cell-type', default=None, required=True, type=str,
                        help="Cell type name")
    args = parser.parse_args()

    download(args.dataset, args.cell_type)
    run_liger()
    upload(args.dataset)

    shutil.rmtree('inputs')
    shutil.rmtree('outputs')


if __name__ == '__main__':
    main()
