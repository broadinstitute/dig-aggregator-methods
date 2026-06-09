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


model_to_gene_stats = {
    'mouse_msigdb_phi1': 'mouse_msigdb',
    'mouse_msigdb_phi5': 'mouse_msigdb'
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


def download_files(dataset, cell_type, model):
    file = f'{s3_in}/out/single_cell/staging/factor_matrix/{dataset}/{cell_type}/{model}/factor_matrix_gene_probs.tsv'
    subprocess.check_call(['aws', 's3', 'cp', f'{file}', 'input/'])


def get_model_data():
    with open(f'{downloaded_files}/aws_pigean_models_s3.json', 'r') as f:
        models = json.load(f)
    return ({model['name']: model for model in models['models']},
            {gene_set['name']: gene_set for gene_set in models['gene_sets']})


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


def make_positive_controls(idx):
    with open('positive_controls_in.txt', 'w') as f_out:
        f_out.write('gene\tprob\n')
        with open('input/factor_matrix_gene_probs.tsv', 'r') as f:
            _ = f.readline()
            for line in f:
                split_line = line.strip().split('\t')
                prob = 0.05 if float(split_line[idx]) < 0.05 else float(split_line[idx])
                f_out.write(f'{split_line[0]}\t{prob}\n')


def run(model, openapi_key):
    with open('input/factor_matrix_gene_probs.tsv', 'r') as f:
        num_cols = len(f.readline().strip().split('\t'))
    import time
    t_tot = time.time()
    for idx in range(1, num_cols):
        t = time.time()
        make_positive_controls(idx)
        output = f'factor_{idx-1}'
        subprocess.run(f'python {downloaded_files}/priors-251215-mod.py naive_factor '
                       f'--gene-map-in {downloaded_files}/portal_gencode.gene.map '
                       f'--max-num-gene-sets 5000 '
                       '--gene-filter-value 1 '
                       '--gene-set-filter-value 0.01 '
                       '--positive-controls-in positive_controls_in.txt '
                       '--positive-controls-id-col gene '
                       '--positive-controls-prob-col prob '
                       f'--positive-controls-all-in {downloaded_files}/NCBI37.3.plink.gene.loc '
                       '--positive-controls-all-id-col 6 '
                       '--positive-controls-all-no-header '
                       f'--gene-stats-out staging/gs.{output}.out '
                       f'--gene-set-stats-out staging/gss.{output}.out '
                       f'--factors-out staging/f.{output}.out '
                       f'--gene-clusters-out staging/gc.{output}.out '
                       f'--pheno-clusters-out staging/pc.{output}.out '
                       f'--gene-set-clusters-out staging/gsc.{output}.out '
                       f'--params-out staging/p.{output}.out '
                       f'--factor-phewas-stats-out staging/fphs.{output}.out '
                       f'--phewas-stats-out staging/phs.{output}.out '
                       f'--gene-set-phewas-stats-in {downloaded_files}/gss_{model_to_gene_stats[model]}.tsv '
                       '--gene-set-phewas-stats-id-col gene_set '
                       '--gene-set-phewas-stats-pheno-col trait '
                       '--gene-set-phewas-stats-beta-uncorrected-col beta_uncorrected '
                       f'--gene-phewas-stats-in {downloaded_files}/gs_{model_to_gene_stats[model]}.tsv '
                       '--gene-phewas-bfs-id-col gene '
                       '--gene-phewas-bfs-pheno-col trait '
                       '--gene-phewas-bfs-combined-col combined '
                       '--gene-phewas-bfs-log-bf-col log_bf '
                       f'--run-phewas-from-gene-phewas-stats-in {downloaded_files}/gs_{model_to_gene_stats[model]}.tsv '
                       f'--factor-phewas-from-gene-phewas-stats-in {downloaded_files}/gs_{model_to_gene_stats[model]}.tsv '
                       + (f'--lmm-auth-key {openapi_key} ' if openapi_key is not None else '')
                       + ' '.join(get_gene_sets(model_to_gene_stats[model])), shell=True)
        print(time.time() - t)
    print(time.time() - t_tot)
    os.remove('positive_controls_in.txt')
    return num_cols


def combine_top_gene_sets():
    with open('input/factor_matrix_gene_loadings.tsv', 'r') as f:
        num_cols = len(f.readline().strip().split('\t'))

    with open('output/pigean.top_gene_sets.tsv', 'w') as f_out:
        f_out.write('factor\tgene_sets\n')
        for idx in range(1, num_cols):
            with open(f'staging/gss.factor_{idx-1}.out', 'r') as f:
                _ = f.readline()
                f_out.write('{}\t{}\n'.format(idx-1, ','.join([f.readline().strip().split('\t')[0] for idx in range(5)])))


def combine_gene_sets():
    with open('input/factor_matrix_gene_loadings.tsv', 'r') as f:
        num_cols = len(f.readline().strip().split('\t'))

    with open('output/pigean.gene_sets.tsv', 'w') as f_out:
        f_out.write('factor\tgene_set\tbeta\n')
        for idx in range(1, num_cols):
            with open(f'staging/gss.factor_{idx-1}.out', 'r') as f:
                header = f.readline()
                for line in f:
                    line_dict = dict(zip(header, line.strip().split('\t')))
                    f_out.write('{}\t{}\t{}\n'.format(
                        idx - 1,
                        line_dict['Gene_Set'],
                        line_dict['beta']
                    ))


def upload(dataset, cell_type, model):
    staging_output = f'{s3_out}/out/single_cell/staging/pigean/{dataset}/{cell_type}/{model}'
    subprocess.check_call('zip factor.results.zip staging/*.out', shell=True)
    subprocess.check_call(f'aws s3 cp pigean.results.zip {staging_output}/', shell=True)

    output = f'{s3_out}/out/single_cell/pigean/{dataset}/{cell_type}/{model}'
    subprocess.check_call(f'aws s3 cp output/ {output}/ --recursive', shell=True)


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--dataset', type=str, required=True)
    opts.add_argument('--cell-type', type=str, required=True)
    opts.add_argument('--model', type=str, required=True)
    args = opts.parse_args()

    open_api_key = OpenAPIKey().get_key()
    download_files(args.dataset, args.cell_type, args.model)

    os.makedirs('staging', exist_ok=True)
    run(args.model, open_api_key)

    combine_top_gene_sets()
    combine_gene_sets()
    upload(args.dataset, args.cell_type, args.model)
    shutil.rmtree('input')
    shutil.rmtree('staging')
    shutil.rmtree('output')


if __name__ == '__main__':
    main()
