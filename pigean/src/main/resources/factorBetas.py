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

def combine_gss():
    if not os.path.exists('gss.out') and os.path.exists('gss.baseline.out'):
        os.rename('gss.baseline.out', 'gss.combined.out')
    elif os.path.exists('gss.out') and not os.path.exists('gss.baseline.out'):
        os.rename('gss.out', 'gss.combined.out')
    elif os.path.exists('gss.out') and os.path.exists('gss.baseline.out'):
        with open('gss.out', 'r') as f:
            output_header = f.readline().strip().split('\t')
        with open('gss.combined.out', 'w') as f_out:
            f_out.write('{}\n'.format('\t'.join(output_header)))
            for file_in in ['gss.out', 'gss.baseline.out']:
                with open(file_in, 'r') as f_in:
                    header = f_in.readline().strip().split('\t')
                    for line in f_in:
                        line_dict = dict(zip(header, line.strip().split('\t')))
                        line_dict['filter_reason'] = line_dict.get('filter_reason', '')
                        f_out.write('{}\n'.format('\t'.join([line_dict[key] for key in output_header])))
        os.remove('gss.out')
        os.remove('gss.baseline.out')


def check_file(file_in):
    return subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0


def download_data(trait_group, phenotype, gene_set_size):
    file_path = f'{s3_in}/out/pigean/staging/pigean/{trait_group}/{phenotype}'
    if gene_set_size != 'mouse_msigdb':
        if check_file(f'{file_path}/{gene_set_size}/gss.out'):
            subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/{gene_set_size}/gss.out', '.'])
    if check_file(f'{file_path}/mouse_msigdb/gs.out'):
        subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/mouse_msigdb/gs.out', 'gs.baseline.out'])
    if check_file(f'{file_path}/mouse_msigdb/gss.out'):
        subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/mouse_msigdb/gss.out', 'gss.baseline.out'])
    combine_gss()


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

def open_ai_cmd(openapi_key):
    if openapi_key is not None:
        return ['--lmm-auth-key', openapi_key, '--lmm-provider', 'openai', '--lmm-model', 'gpt-4o-mini']
    else:
        return []


def run_factor(gene_set_size, openapi_key):
    cmd = [
              'python3.11', '-m', 'eaggl', 'factor',
              '--learn-phi',
              '--gene-set-stats-in', os.path.abspath('gss.combined.out'),
              '--gene-stats-in', os.path.abspath('gs.baseline.out'),
              '--gene-loc-file', f'{downloaded_files}/NCBI37.3.plink.gene.loc',
              '--gene-map-in', f'{downloaded_files}/portal_gencode.gene.map',
              '--factors-out', os.path.abspath('f.out'),
              '--gene-clusters-out', os.path.abspath('gc.out'),
              '--gene-set-clusters-out', os.path.abspath('gsc.out'),
              '--params-out', os.path.abspath('p.out')
          ] + get_gene_sets(gene_set_size) + open_ai_cmd(openapi_key)
    subprocess.check_call(cmd, cwd=f'{downloaded_files}/pigean/src')


def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def upload_data(trait_group, phenotype, gene_set_size):
    file_path = f'{s3_out}/out/pigean/staging/factor/{trait_group}/{phenotype}/{gene_set_size}/'
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
    if os.path.exists('gss.combined.out') and os.path.exists('gs.baseline.out'):
        try:
            run_factor(args.gene_set_size, open_api_key)
            upload_data(args.trait_group, args.phenotype, args.gene_set_size)
        except Exception:
            print('Error')
    if os.path.exists('gs.baseline.out'):
        os.remove('gs.baseline.out')
    if os.path.exists('gss.combined.out'):
        os.remove('gss.combined.out')


if __name__ == '__main__':
    main()



