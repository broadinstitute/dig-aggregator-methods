#!/usr/bin/python3
import argparse
from boto3.session import Session
import json
import os
import subprocess

downloaded_files = '/mnt/var/pigean'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

size_sorting = {
    'gene_set_list_mouse_2024.txt': ['small', 'large'],
    'gene_set_list_msigdb_nohp.txt': ['small', 'large'],
    'gene_set_list_string_notext_medium_processed.txt': ['large'],
    'gene_set_list_pops_sparse_small.txt': ['large'],
    'gene_set_list_mesh_processed.txt': ['large']
}


class OpenAPIKey:
    def __init__(self):
        self.secret_id = 'openapi-key'
        self.region = 'us-east-1'
        self.config = None

    def get_config(self):
        if self.config is None:
            client = Session().client('secretsmanager', region_name=self.region)
            self.config = json.loads(client.get_secret_value(SecretId=self.secret_id)['SecretString'])
        return self.config

    def get_key(self):
        if self.config is None:
            self.config = self.get_config()
        return self.config['apiKey']


def download_data(trait_group, phenotype, gene_set_size):
    file_path = f'{s3_in}/out/pigean/staging/pigean/{trait_group}/{phenotype}/{gene_set_size}'
    subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/gs.out', '.'])
    subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/gss.out', '.'])


def get_gene_sets(gene_set_size):
    size_gene_sets = [gene_set for gene_set, sizes in size_sorting.items() if gene_set_size in sizes]
    if len(size_gene_sets) > 0:
        return [cmd for gene_set in size_gene_sets for cmd in ('--X-in', f'{downloaded_files}/{gene_set}')]
    else:
        raise Exception(f'Invalid gene set size {gene_set_size}')


def run_factor(gene_set_size, openapi_key):
    cmd = [
              'python3', f'{downloaded_files}/priors.py', 'factor',
              '--gene-bfs-in', 'gs.out',
              '--gene-set-stats-in', 'gss.out',
              '--gene-map-in', f'{downloaded_files}/gencode.gene.map',
              '--gene-bfs-id-col', 'Gene',
              '--gene-bfs-log-bf-col', 'log_bf',
              '--gene-bfs-combined-col', 'combined',
              '--gene-bfs-prior-col', 'prior',
              '--gene-set-stats-id-col', 'Gene_Set',
              '--gene-set-stats-beta-uncorrected-col', 'beta_uncorrected',
              '--gene-set-stats-beta-col', 'beta',
              '--min-gene-set-read-beta-uncorrected', '1e-20',
              '--gene-set-filter-type', 'beta_uncorrected',
              '--gene-set-filter-value', '0.01',
              '--gene-filter-type', 'combined',
              '--gene-filter-value', '1',
              '--factors-out', 'f.out',
              '--gene-clusters-out', 'gc.out',
              '--gene-set-clusters-out', 'gsc.out',
              ' --gene-set-multiply-type', 'beta_uncorrected',
          ] + get_gene_sets(gene_set_size) + \
          (['--lmm-auth-key', openapi_key] if openapi_key is not None else [])
    subprocess.check_call(cmd)

def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def upload_data(trait_group, phenotype, gene_set_size):
    file_path = f'{s3_out}/out/pigean/staging/factor/{trait_group}/{phenotype}/{gene_set_size}/'
    for file in ['f.out', 'gc.out', 'gsc.out']:
        subprocess.check_call(['aws', 's3', 'cp', file, file_path])
        os.remove(file)
    success(file_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--trait-group', default=None, required=True, type=str,
                        help="Input phenotype group.")
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    parser.add_argument('--gene-set-size', default=None, required=True, type=str,
                        help="gene-set-size (small, medium, or large).")
    args = parser.parse_args()

    open_api_key = OpenAPIKey().get_key()
    download_data(args.trait_group, args.phenotype, args.gene_set_size)
    try:
        run_factor(args.gene_set_size, open_api_key)
        upload_data(args.trait_group, args.phenotype, args.gene_set_size)
    except Exception:
        print('Error')
    os.remove('gs.out')
    os.remove('gss.out')


if __name__ == '__main__':
    main()



