#!/usr/bin/python3
import argparse
from boto3.session import Session
import json
import os
import shutil
import subprocess

downloaded_files = '/mnt/var/pigean'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


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


def get_model_data():
    with open(f'{downloaded_files}/aws_pigean_models_s3.json', 'r') as f:
        models = json.load(f)
    return ({model['name']: model for model in models['models']},
            {gene_set['name']: gene_set for gene_set in models['gene_sets']})


def download_data(trait_group, phenotype, gene_set_size):
    file_path = f'{s3_in}/out/pigean/staging/pigean/{trait_group}/{phenotype}/{gene_set_size}'
    subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/gs.out', '.'])
    subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/gss.out', '.'])


def get_gene_sets(gene_set_size):
    models, gene_sets = get_model_data()
    model_info = models[gene_set_size]
    inputs = []
    for gene_set in model_info['gene_sets']:
        gene_set_info = gene_sets[gene_set]
        if gene_set_info['type'] == 'set':
            inputs += ['--X-in', f'{downloaded_files}/{gene_set_info["file"]}']
        else:
            inputs += ['--X-list', f'{downloaded_files}/{gene_set_info["name"]}/{gene_set_info["file"]}']
    if len(inputs) > 0:
        return inputs
    else:
        raise Exception(f'Invalid gene set size {gene_set_size}')


def run_factor(gene_set_size, phi, openapi_key):
    cmd = [
              'python3.11', '-m', 'eaggl', 'factor',
              '--learn-phi',
              '--gene-set-stats-in', 'gss.out', # need combine
              '--gene-stats-in', 'gs.out', # base model
              '--gene-loc-file', f'{downloaded_files}/NCBI37.3.plink.gene.loc',
              '--gene-map-in', f'{downloaded_files}/portal_gencode.gene.map',
              '--factors-out', 'f.out',
              '--gene-clusters-out', 'gc.out',
              '--gene-set-clusters-out', 'gsc.out',
              '--params-out', 'p.out'
          ] + get_gene_sets(gene_set_size)
    subprocess.check_call(cmd)


def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def upload_data(trait_group, phenotype, gene_set_size, phi):
    file_path = f'{s3_out}/out/pigean/staging/factor/{trait_group}/{phenotype}/{gene_set_size}___phi{phi}/'
    for file in ['phs.out', 'f.out', 'gc.out', 'pc.out', 'gsc.out', 'p.out']:
        if os.path.exists(file):
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
                        help="gene-set-size (e.g. small)")
    args = parser.parse_args()

    open_api_key = OpenAPIKey().get_key()
    download_data(args.trait_group, args.phenotype, args.gene_set_size)
    download_combined_data(args.gene_set_size)
    try:
        run_factor(args.gene_set_size, args.phi, open_api_key)
        upload_data(args.trait_group, args.phenotype, args.gene_set_size, args.phi)
    except Exception:
        print('Error')
    os.remove('gs.out')
    os.remove('gss.out')
    shutil.rmtree('combined')


if __name__ == '__main__':
    main()



