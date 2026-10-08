#!/usr/bin/python3
import argparse
from boto3.session import Session
import datetime
import glob
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


def download_data(trait_group, phenotype, model):
    file_path = f'{s3_in}/out/falcon/staging/falcon/{trait_group}/{phenotype}/{model}/pigean'
    subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/', 'pigean/', '--recursive'])


def get_gene_sets(model):
    models, gene_sets = get_model_data()
    model_info = models[model]
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
        raise Exception(f'Invalid gene set size {model}')


def open_ai_cmd(openapi_key):
    if openapi_key is not None:
        return ['--lmm-auth-key', openapi_key, '--lmm-provider', 'openai', '--lmm-model', 'gpt-4o-mini']
    else:
        return []


def run_factor(model, openapi_key):
    gs_file = glob.glob('pigean/*.gene_stats.tsv')[0]
    gss_file = glob.glob('pigean/*.gene_set_stats.tsv')[0]
    cmd = [
              'python3.11', '-m', 'eaggl', 'factor',
              '--discovery-model', 'gene_by_gene',
              '--deterministic',
              '--factor-runs', '5',
              '--consensus-nmf',
              '--gene-set-stats-in', os.path.abspath(gss_file),
              '--gene-stats-in', os.path.abspath(gs_file),
              '--factors-out', os.path.abspath('factors.out.gz'),
              '--factor-metrics-out', os.path.abspath('factor_metrics.out.gz'),
              '--consensus-stats-out', os.path.abspath('consensus_stats.out.gz'),
              '--gene-clusters-out', os.path.abspath('gene_clusters.out.gz'),
              '--gene-set-clusters-out', os.path.abspath('gene_set_clusters.out.gz'),
              '--gene-clusters-full-out', os.path.abspath('gene_clusters_full.direct.out.gz'),
              '--gene-clusters-full-via-gene-sets-out', os.path.abspath('gene_clusters_full_via_gene_sets.out.gz'),
              '--params-out', os.path.abspath('params.out'),
              '--warnings-file', os.path.abspath('warnings.txt')
          ] + get_gene_sets(model)
    with open(os.path.abspath('run.log'), 'w') as f:
        subprocess.run(cmd, cwd=f'{downloaded_files}/pigean/src', stdout=f, stderr=f)


def make_metadata(trait_group, phenotype, model, started, finished):
    with open('run_metadata.txt', 'w') as f:
        f.write('repository=https://github.com/flannick/pigean.git\n')
        f.write('commit=ca12beeba4a80fb829695ec992718e05465c4e73\n')
        f.write(f'trait_group={trait_group}\n')
        f.write(f'phenotype={phenotype}\n')
        f.write(f'model={model}\n')
        f.write(f'pigean_intput={s3_in}/out/pigean/staging/pigean/{trait_group}/{phenotype}/{model}\n')
        f.write(f'eaggl_output={s3_out}/out/pigean/staging/eaggl/{trait_group}/{phenotype}/{model}\n')
        f.write(f'started_utc={started}\n')
        f.write(f'completed_utc={finished}\n')
        f.write('status=success\n')


def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def upload_data(trait_group, phenotype, model):
    file_path = f'{s3_out}/out/falcon/staging/eaggl/{trait_group}/{phenotype}/{model}/'
    for file in ['factors.out.gz', 'factor_metrics.out.gz', 'consensus_stats.out.gz',
                 'gene_clusters.out.gz', 'gene_set_clusters.out.gz',
                 'gene_clusters_full.direct.out.gz', 'gene_clusters_full_via_gene_sets.out.gz',
                 'params.out', 'warnings.txt', 'run.log', 'run_metadata.txt']:
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
    parser.add_argument('--model', default=None, required=True, type=str,
                        help="model (e.g. mouse_msigdb)")
    args = parser.parse_args()

    open_api_key = OpenAPIKey().get_key()
    download_data(args.trait_group, args.phenotype, args.model)
    try:
        started = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        run_factor(args.model, open_api_key)
        finished = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        make_metadata(args.trait_group, args.phenotype, args.model, started, finished)
        upload_data(args.trait_group, args.phenotype, args.model)
    except Exception:
        print('Error')
    shutil.rmtree('pigean')


if __name__ == '__main__':
    main()



