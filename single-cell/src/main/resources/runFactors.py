#!/usr/bin/python3
import argparse
from boto3.session import Session
import json
import os
import requests
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


class LLMSecrets:
    def __init__(self):
        self.secret_id = 'ollama-key'
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

    def get_endpoint(self):
        if self.config is None:
            self.config = self.get_config()
        return self.config['internalEndpoint']


def translate_gene_loading_data(dataset, cell_type, model):
    file_in = f'{s3_in}/out/single_cell/staging/factor_matrix/{dataset}/{cell_type}/{model}/factor_matrix_gene_loadings.tsv'
    if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
        with open('outputs/factor_genes.json', 'w') as f_out:
            with open('inputs/factor_matrix_gene_loadings.tsv', 'r') as f:
                header = f.readline().strip().split('\t')
                factor_values = {factor: [] for factor in header[1:]}
                for line in f:
                    gene, factor_data = line.strip().split('\t', 1)
                    v = list(map(float, factor_data.split('\t')))
                    if sum(v) > 0:
                        json_line = dict(zip(header[1:], v))
                        for factor in json_line:
                            if json_line[factor] > 0:
                                f_out.write(json.dumps(
                                    {
                                        'dataset': dataset,
                                        'cell_type': cell_type,
                                        'model': model,
                                        'factor': factor,
                                        'gene': gene,
                                        'value': json_line[factor]
                                    }
                                ) + '\n')
                            factor_values[factor].append((json_line[factor], gene))


def translate_cell_loading_data(dataset, cell_type, model):
    file_in = f'{s3_in}/out/single_cell/staging/factor_matrix/{dataset}/{cell_type}/{model}/factor_matrix_cell_loadings.tsv'
    if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
        with open('outputs/factor_cells.json', 'w') as f_out:
            with open('inputs/factor_matrix_cell_loadings.tsv', 'r') as f:
                header = f.readline().strip().split('\t')
                for line in f:
                    cell, _, factor_data = line.strip().split('\t', 2)
                    v = list(map(float, factor_data.split('\t')))
                    if sum(v) > 0:
                        json_line = dict(zip(header[2:], v))
                        for factor in json_line:
                            if json_line[factor] > 0:
                                f_out.write(json.dumps(
                                    {
                                        'dataset': dataset,
                                        'cell_type': cell_type,
                                        'model': model,
                                        'factor': factor,
                                        'cell': cell,
                                        'value': json_line[factor]
                                    }
                                ) + '\n')


def translate_factors(dataset, cell_type, model, factor_data):
    factor_map = {factor['factor']: factor for factor in factor_data}
    file_in = f'{s3_in}/out/single_cell/staging/factor_matrix/{dataset}/{cell_type}/{model}/factor_matrix_factors.tsv'
    if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
        with open('outputs/factors.json', 'w') as f_out:
            with open('inputs/factor_matrix_factors.tsv', 'r') as f:
                header = f.readline().strip().split('\t')
                for line in f:
                    json_line = dict(zip(header, line.strip().split('\t')))
                    factor = json_line['factor']
                    f_out.write(json.dumps(
                        {
                            'dataset': dataset,
                            'cell_type': cell_type,
                            'model': model,
                            'factor': factor,
                            'importance': float(json_line['exp_lambdak']),
                            'top_cells': json_line['top_cells'],
                            'top_genes': factor_map[factor]['top_genes'],
                            'top_gene_sets': factor_map[factor]['top_gene_sets'],
                            'top_traits': factor_map[factor]['top_traits'],
                            'label': factor_map[factor]['labels'].get('traits', '')
                        }
                    ) + '\n')


def translate_data(dataset, cell_type, model, factor_data):
    translate_gene_loading_data(dataset, cell_type, model)
    translate_cell_loading_data(dataset, cell_type, model)
    translate_factors(dataset, cell_type, model, factor_data)


def get_gene_data(dataset, cell_type, model):
    file_in = f'{s3_in}/out/single_cell/staging/factor_matrix/{dataset}/{cell_type}/{model}/factor_matrix_factors.tsv'
    factor_data = {}
    if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
        with open('inputs/factor_matrix_factors.tsv', 'r') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                json_line = dict(zip(header, line.strip().split('\t')))
                factor_data[json_line['factor']] = {
                    'importance': float(json_line['exp_lambdak']),
                    'top_genes': json_line['top_genes'].split(',')
                }
    return factor_data

def get_gene_loading_data(dataset, cell_type, model):
    file_in = f'{s3_in}/out/single_cell/staging/factor_matrix/{dataset}/{cell_type}/{model}/factor_matrix_gene_loadings.tsv'
    gene_loading_data = {}
    if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
        with open('inputs/factor_matrix_gene_loadings.tsv', 'r') as f:
            header = f.readline().strip().split('\t')
            for factor_key in header[1:]:
                gene_loading_data[factor_key] = []
            for line in f:
                gene, factor_data = line.strip().split('\t', 1)
                for factor_key, factor_value in dict(zip(header[1:], factor_data.split('\t'))).items():
                    gene_loading_data[factor_key].append((float(factor_value), gene))
    top_50_genes = {}
    for factor_key, gene_data in gene_loading_data.items():
        top_50_genes[factor_key] = [gene_datum[1] for gene_datum in sorted(gene_data, reverse=True)[:50]]
    return top_50_genes

def get_gene_set_data(dataset, cell_type, model):
    file_in = f'{s3_in}/out/single_cell/pigean/{dataset}/{cell_type}/{model}/pigean.gene_sets.tsv'
    factor_data = {}
    if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
        with open('inputs/pigean.gene_sets.tsv', 'r') as f:
            _ = f.readline().strip().split('\t')
            for line in f:
                factor, gene_set, beta, beta_uncorrected = line.strip().split('\t')
                if factor not in factor_data:
                    factor_data[factor] = []
                factor_data[factor].append((float(beta), gene_set))
    return {factor: [gene_set for value, gene_set in sorted(values, reverse=True)[:20]] for factor, values in factor_data.items()}


def get_trait_display_map():
    trait_display_map = {}
    #  TODO: Something more consistent and permanent, this is from the cfde bioindex, but I moved it to bin
    file = 's3://dig-analysis-bin/pigean/misc/trait_data_cfde.json'
    subprocess.check_call(['aws', 's3', 'cp', file, 'inputs/'])
    with open('inputs/trait_data_cfde.json', 'r') as f:
        for line in f:
            json_line = json.loads(line.strip())
            trait_display_map[json_line['phenotype']] = json_line['phenotype_name']
    return trait_display_map


def get_trait_data(dataset, cell_type, model):
    file_in = f'{s3_in}/out/single_cell/staging/factor_phewas/{dataset}/{cell_type}/{model}/phewas_gene_loadings.txt'
    factor_data = {}
    trait_display_map = get_trait_display_map()
    if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
        with open('inputs/phewas_gene_loadings.txt', 'r') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                json_line = dict(zip(header, line.strip().split('\t')))
                p = float(json_line['P'])
                if json_line['Factor'] not in factor_data:
                    factor_data[json_line['Factor']] = []
                trait = trait_display_map.get(json_line['Pheno'], json_line['Pheno'])
                factor_data[json_line['Factor']].append((p, trait))
    return {factor: [trait for value, trait in sorted(values)[:20]] for factor, values in factor_data.items()}


def get_data(dataset, cell_type, model):
    gene_data = get_gene_data(dataset, cell_type, model)
    gene_loading_data = get_gene_loading_data(dataset, cell_type, model)
    gene_set_data = get_gene_set_data(dataset, cell_type, model)
    trait_data = get_trait_data(dataset, cell_type, model)
    factors = list(gene_data.keys())# | gene_set_data.keys() | trait_data.keys())
    return [{
        'factor': factor,
        'importance': gene_data.get(factor, {}).get('importance'),
        'top_genes': gene_loading_data.get(factor, []),
        'top_gene_sets': gene_set_data.get(factor, []),
        'top_traits': trait_data.get(factor, []),
        'labels': {}
    } for factor in factors]


class LLMEndpoint:
    def __init__(self, llm_endpoint, auth_key):
        self.llm_endpoint = llm_endpoint
        self.auth_key = auth_key

    def query(self, query):
        print(query)
        headers = {
            'Content-Type': 'application/json',
            'X-API-Key': self.auth_key
        }

        json_data = {
            'userPrompt': query,
            'systemPrompt': 'You are a computational biologist. Be concise.'
        }
        try:
            response = requests.post(f'{self.llm_endpoint}/ollama', headers=headers, json=json_data).json()
            return response['data'][0]['ollama_response'].strip()
        except Exception:
            print("LMM call failed; returning None")
            return None


def format_response(response):
    return response \
        .strip() \
        .replace('\n', ' ') \
        .replace('\u2013', '-') \
        .replace('\u2014', '-') \
        .replace('*', '') \
        .encode('utf-8') \
        .decode('ascii', errors='ignore')


def label_factor(dataset, cell_type, factor_data, llm_endpoint):
    for label_type in ['genes', 'gene_sets', 'traits']:
        key = f'top_{label_type}'
        filtered_data = [data for data in factor_data if len(data[key]) > 0]
        if len(filtered_data) > 0:
            for i, data in enumerate(filtered_data):
                prompt_data = '{} {} - Top {}: {}'.format(
                    dataset,
                    cell_type,
                    label_type,
                    ', '.join(data[key])
                )
                prompt = 'Create a concise biological label (2–6 words) for this gene-set/trait group. ' \
                         'Do not just restate the genes, gene sets, or traits, but provide a description of its function or mechanism. ' \
                         f'Return ONLY the label summary.\n\n{prompt_data}'
                response = llm_endpoint.query(prompt)
                if response is not None:
                    print(format_response(response))
                    factor_data[i]['labels'][label_type] = format_response(response)
    return factor_data


def upload_data(dataset, cell_type, model):
    path = f'{s3_out}/out/single_cell/factors/{dataset}/{cell_type}/{model}/'
    subprocess.check_call(['aws', 's3', 'cp', 'outputs/', path, '--recursive'])


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--dataset', type=str, required=True)
    opts.add_argument('--cell-type', type=str, required=True)
    opts.add_argument('--model', type=str, required=True)
    args = opts.parse_args()

    llm_secrets = LLMSecrets()
    llm_endpoint = LLMEndpoint(llm_secrets.get_endpoint(), llm_secrets.get_key())
    factor_data = get_data(args.dataset, args.cell_type, args.model)
    factor_data = label_factor(args.dataset, args.cell_type, factor_data, llm_endpoint)

    os.makedirs('outputs', exist_ok=True)
    translate_data(args.dataset, args.cell_type, args.model, factor_data)
    upload_data(args.dataset, args.cell_type, args.model)
    shutil.rmtree('inputs')
    shutil.rmtree('outputs')


if __name__ == '__main__':
    main()
