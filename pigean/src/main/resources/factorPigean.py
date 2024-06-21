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
    'gene_set_list_mouse.txt': ['small', 'medium', 'large'],
    'gene_set_list_msigdb_nohp.txt': ['small', 'medium', 'large'],
    'gene_set_list_string_notext_medium_processed.txt': ['medium', 'large'],
    'gene_set_list_pops_sparse_small.txt': ['medium', 'large'],
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


def download_data(phenotype, sigma, gene_set_size):
    file_path = f'{s3_in}/out/pigean/staging/pigean/{phenotype}/sigma={sigma}/size={gene_set_size}/'
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
              '--gene-stats-in', 'gs.out',
              '--gene-set-stats-in', 'gss.out',
              '--gene-loc-file', f'{downloaded_files}/NCBI37.3.plink.gene.loc',
              '--gene-map-in', f'{downloaded_files}/gencode.gene.map',
              '--debug-level', '3',
              '--factors-out', 'f.out',
              '--marker-factors-out', 'mf.out',
              '--gene-factors-out', 'gf.out',
              '--gene-set-factors-out', 'gsf.out',
              '--gene-clusters-out', 'gc.out',
              '--gene-set-clusters-out', 'gsc.out',
              '--hide-opts'
          ] + get_gene_sets(gene_set_size) + \
          ['--lmm-auth-key', openapi_key] if openapi_key is not None else []
    print(' '.join(cmd))
    subprocess.check_call(cmd)


def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def upload_data(phenotype, sigma, gene_set_size):
    file_path = f'{s3_out}/out/pigean/staging/factor/{phenotype}/sigma={sigma}/size={gene_set_size}/'
    for file in ['f.out', 'mf.out', 'gf.out', 'gsf.out', 'gc.out', 'gsc.out']:
        subprocess.check_call(['aws', 's3', 'cp', file, file_path])
        os.remove(file)
    success(file_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    parser.add_argument('--sigma', default=None, required=True, type=str,
                        help="Sigma power (0, 2, 4).")
    parser.add_argument('--gene-set-size', default=None, required=True, type=str,
                        help="gene-set-size (small, medium, or large).")
    args = parser.parse_args()

    open_api_key = None#OpenAPIKey().get_key()
    download_data(args.phenotype, args.sigma, args.gene_set_size)
    try:
        run_factor(args.gene_set_size, open_api_key)
        upload_data(args.phenotype, args.sigma, args.gene_set_size)
        os.remove('gs.out')
        os.remove('gss.out')
    except:
        print('ERROR')


if __name__ == '__main__':
    main()



