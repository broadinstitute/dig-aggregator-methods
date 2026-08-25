#!/usr/bin/python3
import argparse
import os
import shutil
import subprocess

downloaded_files = '/mnt/var/single_cell'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_data(dataset, cell_type):
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/out/single_cell/staging/split/{dataset}/{cell_type}/norm_counts.tsv.gz', 'inputs/'])
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/out/single_cell/staging/split/{dataset}/{cell_type}/norm_counts.metadata.tsv.gz', 'inputs/'])


def prepare_sparse_matrix():
    cmd = [
        'python3.11', f'{downloaded_files}/dig-cell-state-scoring/scripts/convert_expression_tsv_to_sparse_10x.py',
        '--matrix-tsv', 'inputs/norm_counts.tsv.gz',
        '--out-dir', 'outputs/rank_10x',
        '--orientation', 'gene_by_cell',
        '--value-type', 'log1p_cp10k'
    ]
    subprocess.check_call(cmd)


def upload_data(dataset, cell_type):
    subprocess.check_call(['aws', 's3', 'cp', 'outputs/rank_10x', f'{s3_in}/out/single_cell/staging/mtx/{dataset}/{cell_type}/', '--recursive'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset')
    parser.add_argument('--cell-type')
    args = parser.parse_args()

    download_data(args.dataset, args.cell_type)
    prepare_sparse_matrix()
    upload_data(args.dataset, args.cell_type)
    shutil.rmtree('outputs')
    shutil.rmtree('inputs')


if __name__ == '__main__':
    main()
