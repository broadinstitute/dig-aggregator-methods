#!/usr/bin/python3
import argparse
from boto3.session import Session
import json
import os
import re
import requests
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


class LLMSecrets:
    def __init__(self):
        self.secret_id = 'bedrock-key'
        self.region = 'us-east-1'
        self.config = None

    def get_config(self):
        if self.config is None:
            client = Session().client('secretsmanager', region_name=self.region)
            self.config = json.loads(client.get_secret_value(SecretId=self.secret_id)['SecretString'])
        return self.config


    def get_endpoint(self):
        if self.config is None:
            self.config = self.get_config()
        return self.config['internalEndpoint']


prompt = '''
I have identified gene programs/factors from single-cell RNA-seq data for {cell_type} cells in {tissue}. I will provide a table containing the top genes for each factor/program, typically with columns such as:

factor
rank
gene
loading
Please biologically annotate and summarize each factor.
For each factor, provide a table with the following columns:
1. Factor
2. Suggested program name
3. Representative genes
4. Biological interpretation
5. Assessment — classify the factor as one of:
Strong cell-intrinsic biological program
Plausible cell-intrinsic program, but lower confidence
Cell-state/subtype-associated program
Generic stress/dissociation/technical program
Contamination/ambient RNA/doublet from another cell type
Annotation principles
Interpret the factors primarily as coordinated biological processes occurring within {cell_type}, rather than automatically assigning every factor to a distinct cell subtype.
For each factor:

Base the annotation on the combination of genes, not individual marker genes.
Give greater weight to genes with the highest rankings/loadings.
Identify the core biological process represented by the factor.
Highlight approximately 5–12 representative genes that best support the interpretation.
When appropriate, distinguish a functional program from a cell identity/state signature.
If a program appears to correspond to a known activation, differentiation, metabolic, signaling, stress, secretory, inflammatory, antigen-presentation, proliferative, or other biological process, describe that process explicitly.
If several interpretations are plausible, state the most likely interpretation and briefly note the uncertainty.
Do not force a biological interpretation when the genes do not form a convincing coherent program.
Detect technical or contaminating factors
Pay particular attention to factors that may not represent genuine biology of {cell_type}.
Explicitly identify factors likely caused by:

ambient RNA from abundant cells in the tissue
doublets
contamination by another immune/stromal/parenchymal cell type
mitochondrial or ribosomal expression
cell-cycle effects
dissociation stress
immediate-early response induced during tissue processing
generic housekeeping/transcription/translation programs
For suspected contamination, identify the likely source cell type based on the genes. For example, a liver immune-cell analysis might contain hepatocyte, erythroid, endothelial, myeloid, T-cell, NK-cell, or stellate-cell signatures.
Do not interpret a strong lineage-contamination signature as a novel state of {cell_type}.
Tissue context
Interpret the programs in the context of {tissue}. Where relevant, explain whether the tissue environment could plausibly contribute to the observed program.
However, do not label a factor as tissue-specific unless the genes provide evidence for that interpretation.
Final synthesis
After annotating all factors, provide a short section called “Main {cell_type} programs”.
In this section:

Identify the factors that are the strongest and most interpretable intrinsic programs.
Group closely related factors when appropriate.
Give each retained program a concise biological name.
Identify lower-confidence programs separately.
List factors that should probably be excluded from downstream biological interpretation because they represent contamination or technical effects.
Where useful, distinguish between:

core cell identity/function
signaling or activation
metabolism
stress/adaptation
secretory or biosynthetic activity
differentiation
immune effector function
cellular maintenance
technical/contaminating signals
The goal is to generate a concise biological interpretation suitable for building a general model of cellular programs and states, rather than simply assigning cluster labels.

Output ONLY A TAB-DELIMITED TABLE with the following fields: factor, label, representative_genes, interpretation, assessment
Here is the factor/gene-loading table:
{top_gene_data}
'''


def translate_gene_loading_data(tissue, cell_type, dataset):
    file_in = f'{s3_in}/out/single_cell/staging/factor_matrix/{tissue}/{cell_type}/{dataset}/factor_matrix_gene_loadings.tsv'
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
                                        'tissue': tissue,
                                        'cell_type': cell_type,
                                        'dataset': dataset,
                                        'factor': factor,
                                        'gene': gene,
                                        'value': json_line[factor]
                                    }
                                ) + '\n')
                            factor_values[factor].append((json_line[factor], gene))


def translate_cell_loading_data(tissue, cell_type, dataset):
    file_in = f'{s3_in}/out/single_cell/staging/factor_matrix/{tissue}/{cell_type}/{dataset}/factor_matrix_cell_loadings.tsv'
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
                                        'tissue': tissue,
                                        'cell_type': cell_type,
                                        'dataset': dataset,
                                        'factor': factor,
                                        'cell': cell,
                                        'value': json_line[factor]
                                    }
                                ) + '\n')


def translate_factors(tissue, cell_type, dataset, factor_data):
    file_in = f'{s3_in}/out/single_cell/staging/factor_matrix/{tissue}/{cell_type}/{dataset}/factor_matrix_factors.tsv'
    if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
        with open('outputs/factors.json', 'w') as f_out:
            with open('inputs/factor_matrix_factors.tsv', 'r') as f:
                header = f.readline().strip().split('\t')
                for line in f:
                    json_line = dict(zip(header, line.strip().split('\t')))
                    factor = json_line['factor']
                    output_data = {
                        'tissue': tissue,
                        'cell_type': cell_type,
                        'dataset': dataset,
                        'factor': factor
                    }
                    output_data.update(factor_data[factor])
                    f_out.write(json.dumps(output_data) + '\n')


def translate_data(tissue, cell_type, dataset, factor_data):
    translate_gene_loading_data(tissue, cell_type, dataset)
    translate_cell_loading_data(tissue, cell_type, dataset)
    translate_factors(tissue, cell_type, dataset, factor_data)


def get_importance_data(tissue, cell_type, dataset):
    file_in = f'{s3_in}/out/single_cell/staging/factor_matrix/{tissue}/{cell_type}/{dataset}/factor_matrix_factors.tsv'
    factor_data = {}
    if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
        with open('inputs/factor_matrix_factors.tsv', 'r') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                json_line = dict(zip(header, line.strip().split('\t')))
                factor_data[json_line['factor']] = float(json_line['exp_lambdak'])
    return factor_data


def get_top_genes_data(tissue, cell_type, dataset):
    file_in = f'{s3_in}/out/single_cell/staging/nmf/liger/{tissue}/{cell_type}/{dataset}/top_genes_per_factor.csv'
    output_data = ''
    if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
        with open('inputs/top_genes_per_factor.csv', 'r') as f:
            output_data += f.readline()
            for line in f:
                output_data += line.replace('Factor_factor', 'Factor')
    return output_data


def get_data(tissue, cell_type, dataset):
    importance_data = get_importance_data(tissue, cell_type, dataset)
    factors = list(importance_data.keys())
    return {factor: {
        'factor': factor,
        'importance': importance_data.get(factor),
        'label': {}
    } for factor in factors}


class LLMEndpoint:
    def __init__(self, llm_endpoint):
        self.llm_endpoint = llm_endpoint

    def query(self, query):
        headers = {
            'Content-Type': 'application/json'
        }

        json_data = {
            'userPrompt': query,
            'systemPrompt': 'You are a computational biologist. Be concise.'
        }
        try:
            response = requests.post(f'{self.llm_endpoint}', headers=headers, json=json_data).json()
            return response['data'][0]['bedrock_response'].strip()
        except Exception:
            print("LMM call failed; returning None")
            return None


def label_factor(tissue, cell_type, dataset, factor_data, llm_endpoint):
    response = llm_endpoint.query(
        prompt.format(
            tissue=tissue,
            cell_type=cell_type,
            top_gene_data=get_top_genes_data(tissue, cell_type, dataset)
        )
    )
    print(response)

    split_response = re.search(r'((.*\t){4}.*\n)+', response)[0].strip().split('\n')
    header = split_response[0].strip().split('\t')
    for line in split_response[1:]:
        dict_line = dict(zip(header, line.strip().split('\t')))
        factor_data[dict_line['factor']].update(dict_line)
    return factor_data


def upload_data(tissue, cell_type, dataset):
    path = f'{s3_out}/out/single_cell/factors/{tissue}/{cell_type}/{dataset}/'
    subprocess.check_call(['aws', 's3', 'cp', 'outputs/', path, '--recursive'])


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--tissue', type=str, required=True)
    opts.add_argument('--cell-type', type=str, required=True)
    opts.add_argument('--dataset', type=str, required=True)
    args = opts.parse_args()

    llm_secrets = LLMSecrets()
    llm_endpoint = LLMEndpoint(llm_secrets.get_endpoint())
    factor_data = get_data(args.tissue, args.cell_type, args.dataset)
    factor_data = label_factor(args.tissue, args.cell_type, args.dataset, factor_data, llm_endpoint)

    os.makedirs('outputs', exist_ok=True)
    translate_data(args.tissue, args.cell_type, args.dataset, factor_data)
    upload_data(args.tissue, args.cell_type, args.dataset)
    shutil.rmtree('inputs')
    shutil.rmtree('outputs')


if __name__ == '__main__':
    main()
