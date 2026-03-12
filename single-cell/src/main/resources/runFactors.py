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


class LLMAPIKey:
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
    description_map = {factor['factor']: factor['labels'].get('overall', '') for factor in factor_data}
    label_map = {factor['factor']: factor['labels'].get('label', '') for factor in factor_data}
    file_in = f'{s3_in}/out/single_cell/staging/factor_matrix/{dataset}/{cell_type}/{model}/factor_matrix_factors.tsv'
    if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
        with open('outputs/factors.json', 'w') as f_out:
            with open('inputs/factor_matrix_factors.tsv', 'r') as f:
                header = f.readline().strip().split('\t')
                for line in f:
                    json_line = dict(zip(header, line.strip().split('\t')))
                    factor = 'Factor_{}'.format(json_line['factor_index'])
                    f_out.write(json.dumps(
                        {
                            'dataset': dataset,
                            'cell_type': cell_type,
                            'model': model,
                            'factor': factor,
                            'importance': float(json_line['exp_lambdak']),
                            'top_genes': json_line['top_genes'],
                            'top_cells': json_line['top_cells'],
                            'description': description_map[factor],
                            'label': label_map[factor]
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
                factor_data['Factor_{}'.format(json_line['factor_index'])] = {
                    'importance': float(json_line['exp_lambdak']),
                    'top_genes': json_line['top_genes'].split(',')
                }
    return factor_data


def get_gene_set_data(dataset, cell_type, model):
    file_in = f'{s3_in}/out/single_cell/pigean/{dataset}/{cell_type}/{model}/pigean.gene_sets.tsv'
    factor_data = {}
    if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
        with open('inputs/pigean.gene_sets.tsv', 'r') as f:
            _ = f.readline().strip().split('\t')
            for line in f:
                factor_num, gene_set, beta = line.strip().split('\t')
                factor = 'Factor_{}'.format(factor_num)
                if factor not in factor_data:
                    factor_data[factor] = []
                factor_data[factor].append((float(beta), gene_set))
    return {factor: [gene_set for value, gene_set in sorted(values, reverse=True)[:5]] for factor, values in factor_data.items()}


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
                p = max([float(json_line['P']), float(json_line['P_robust'])])
                if json_line['Factor'] not in factor_data:
                    factor_data[json_line['Factor']] = []
                trait = trait_display_map.get(json_line['Pheno'], json_line['Pheno'])
                factor_data[json_line['Factor']].append((p, trait))
    return {factor: [trait for value, trait in sorted(values)[:5]] for factor, values in factor_data.items()}


def get_data(dataset, cell_type, model):
    gene_data = get_gene_data(dataset, cell_type, model)
    # gene_set_data = get_gene_set_data(dataset, cell_type, model)
    # trait_data = get_trait_data(dataset, cell_type, model)
    factors = list(gene_data.keys())# | gene_set_data.keys() | trait_data.keys())
    return [{
        'factor': factor,
        'importance': gene_data.get(factor, {}).get('importance'),
        'top_genes': gene_data.get(factor, {}).get('top_genes', []),
        # 'top_gene_sets': gene_set_data.get(factor, []),
        # 'top_traits': trait_data.get(factor, []),
        'labels': {}
    } for factor in factors]


def query_lmm(query, auth_key):
    headers = {
        'Content-Type': 'application/json',
        'X-API-Key': auth_key
    }

    json_data = {
        'userPrompt': query,
        'systemPrompt': 'You are a computational biologist. Be concise.'
    }
    try:
        response = requests.post('https://llm.hugeamp.org/ollama', headers=headers, json=json_data).json()
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


def label_factor(dataset, cell_type, model, factor_data, llm_auth_key):
    filtered_data = [data for data in factor_data if len(data['top_genes']) > 0]
    if len(filtered_data) > 0:
        prompt_data = []
        for i, data in enumerate(filtered_data):
            prompt_data.append('{}. {} {} {} - Top Genes: {}'.format(
                i + 1,
                dataset,
                cell_type,
                model,
                ', '.join(data['top_genes'])
            ))
            prompt = ('Create a concise biological label (2–6 words) for this gene-set group. '
                      'Return ONLY the label summary. Top Genes: {}').format(', '.join(data['top_genes']))
            response = query_lmm(prompt, llm_auth_key)
            if response is not None:
                factor_data[i]['labels']['label'] = format_response(response)
    return factor_data


def label_factor_by_type(dataset, cell_type, model, factor_data, llm_auth_key, llm_model):
    for label_type in ['genes', 'gene_sets', 'traits']:
        filtered_data = [data for data in factor_data if len(data['top_{}'.format(label_type)]) > 0]
        if len(filtered_data) > 0:
            prompt_data = []
            for i, data in enumerate(filtered_data):
                prompt_data.append('{}. {} {} {} - Top {}: {}'.format(
                    i + 1,
                    dataset,
                    cell_type,
                    model,
                    label_type,
                    ', '.join(data['top_{}'.format(label_type)])
                ))
            prompt = ('Print a short paragraph description for each group. '
                      'Do not just restate the items in the group, but highlight what stands out and distinguishes the '
                      'group from each other such that the description can be used to understand the function of the '
                      'factor being explored. Print only the description, one per line, label number followed by text: {}').format(
                '\n' + '\n\n'.join(prompt_data)
            )
            response = query_lmm(prompt, llm_auth_key, lmm_model=llm_model)
            if response is not None:
                try:
                    responses = response.strip('\n').split('\n')
                    responses = [x for x in responses if len(x) > 0]

                    if len(responses) == len(factor_data):
                        for i in range(len(factor_data)):
                            cur_response = responses[i]
                            cur_response_tokens = cur_response.split()
                            if len(cur_response_tokens) > 1 and cur_response_tokens[0][-1] == '.':
                                try:
                                    cur_response = ' '.join(cur_response_tokens[1:])
                                except ValueError:
                                    pass
                            factor_data[i]['labels'][label_type] = format_response(cur_response)
                    else:
                        raise Exception
                except Exception:
                    print("Couldn't decode LMM response %s; using simple label" % response)
                    pass
    return factor_data


def combine_descriptions(factor_data, llm_auth_key, llm_model):
    for i in range(len(factor_data)):
        descriptions = list(factor_data[i]['labels'].values())
        if len(descriptions) > 0:
            if len(descriptions) == 1:
                factor_data[i]['labels']['overall'] = descriptions[0]
            else:
                prompt = ('Here are multiple short paragraph descriptions for a factor group. '
                          'Output a single overall paragraph description based off of these descriptions. '
                          'Print only a single paragraph, no line breaks, and do not simply summarize each description, '
                          'but rather produce a new description reflecting the underlying mechanism the group as a whole represents. '
                          'The individual descriptions are:\n\n{}'.format(
                    '\n\n'.join(descriptions)
                ))
                response = query_lmm(prompt, llm_auth_key, lmm_model=llm_model)
                factor_data[i]['labels']['overall'] = format_response(response)

    return factor_data

def label_description(factor_data, llm_auth_key, llm_model):
    for i in range(len(factor_data)):
        overall_description = factor_data[i]['labels'].get('overall')
        if overall_description is not None:
            prompt = ('Here is a short paragraph description for a factor group. '
                      'Output a short at most 5 word label based on this description. '
                      'Print only the label. The description is:\n\n{}'.format(
                overall_description
            ))
            response = query_lmm(prompt, llm_auth_key, lmm_model=llm_model)
            factor_data[i]['labels']['label'] = format_response(response)

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

    llm_api_key = LLMAPIKey().get_key()
    factor_data = get_data(args.dataset, args.cell_type, args.model)
    # factor_data = label_factor_by_type(args.dataset, args.cell_type, args.model, factor_data, open_api_key, 'gpt-5-nano')
    # factor_data = combine_descriptions(factor_data, open_api_key, 'gpt-5-nano')
    # factor_data = label_description(factor_data, open_api_key, 'gpt-5-nano')

    factor_data = label_factor(args.dataset, args.cell_type, args.model, factor_data, llm_api_key)

    os.makedirs('outputs', exist_ok=True)
    translate_data(args.dataset, args.cell_type, args.model, factor_data)
    upload_data(args.dataset, args.cell_type, args.model)
    shutil.rmtree('inputs')
    shutil.rmtree('outputs')


if __name__ == '__main__':
    main()
