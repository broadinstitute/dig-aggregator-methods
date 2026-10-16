#!/usr/bin/python3
import argparse
import glob
import gzip
import os
import re
import shutil
import subprocess

downloaded_files = '/mnt/var/pigean'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_data(trait_group, phenotype, model):
    file_path = f'{s3_in}/out/falcon/staging/eaggl/{trait_group}/{phenotype}/{model}'
    subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/gene_clusters.out.gz', '.'])


def download_gs(model):
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/out/falcon/staging/combined/gs_{model}.tsv', 'gs.tsv'])


def get_factor_cols():
    with gzip.open('gene_clusters.out.gz', 'rt') as f:
        return [a for a in f.readline().strip().split('\t') if re.match(r'Factor[0-9]*', a) is not None]


def run_phewas():
    cmd = [
        'python3', f'{downloaded_files}/factor_phewas.py',
        '--factors-in', 'gene_clusters.out.gz',
        '--factors-gene-id-col', 'Gene',
        '--factors-gene-factor-cols', ','.join(get_factor_cols()),
        '--filter-to-factor-genes',
        '--gene-stats-in', 'gs.tsv',
        '--gene-stats-id-col', 'gene',
        '--gene-stats-pheno-col', 'trait',
        '--gene-stats-assoc-stat-col', 'huge',
        '--output-file', f'phewas.out',
        '--output-provenance-file', f'phewas.provenance.out',
        '--log-file', f'phewas.log'
    ]
    subprocess.check_call(cmd)


def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def upload_data(trait_group, phenotype, model):
    file_path = f'{s3_out}/out/falcon/staging/phewas/{trait_group}/{phenotype}/{model}/'
    for file in ['phewas.out', 'phewas.provenance.out', 'phewas.log']:
        subprocess.check_call(['aws', 's3', 'cp', file, file_path])
        os.remove(file)
    success(file_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--trait-group', default=None, required=True, type=str,
                        help="Input phenotype group.")
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    parser.add_argument('--model', default=None, required=True, type=str,
                        help="model (e.g. mouse_msigdb)")
    args = parser.parse_args()

    download_data(args.trait_group, args.phenotype, args.model)
    download_gs(args.model)
    try:
        run_phewas()
        upload_data(args.trait_group, args.phenotype, args.model)
    except Exception as e:
        print(e)
        print('Error')
    os.remove('gene_clusters.out.gz')
    os.remove('gs.tsv')


if __name__ == '__main__':
    main()
