#!/usr/bin/python3
import gzip
import json
import math
import numpy as np
import os
import re
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def get_dataset_celltype_models():
    dataset_celltype_models = set()
    pattern = r'.*out/single_cell/staging/factor_matrix/([^/]*)/([^/]*)/([^/]*)/factor_matrix_factors.tsv'
    output = subprocess.check_output(['aws', 's3', 'ls', f'{s3_in}/out/single_cell/staging/factor_matrix/', '--recursive']).decode()
    for file in output.split('\n'):
        if 'factor_matrix_factors.tsv' in file:
            dataset_celltype_models |= {re.findall(pattern, file)[0]}
    return list(dataset_celltype_models)


nodes = {}
def get_node_id(node_type, key):
    if node_type not in nodes:
        nodes[node_type] = {}
    if key not in nodes[node_type]:
        nodes[node_type][key] = '{}_{}'.format(node_type, len(nodes[node_type]))
    return nodes[node_type][key]


def process_norm(dataset_celltype_models):
    dataset_celltypes = list(set([(dataset, celltype) for dataset, celltype, _ in dataset_celltype_models]))
    outputs = {
        'cell_to_genes': gzip.open('outputs/cell_to_genes.json.gz', 'wt'),
        'gene_to_cells': gzip.open('outputs/gene_to_cells.json.gz', 'wt')
    }
    for dataset, cell_type in dataset_celltypes:
        file_in = f'{s3_in}/out/single_cell/staging/downsample/{dataset}/{cell_type}/norm_counts.sample.tsv.gz'
        if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
            subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
            with gzip.open('inputs/norm_counts.sample.tsv.gz', 'rt') as f:
                cells = f.readline().strip().split('\t')[1:]
                for line in f:
                    split_data = line.strip().split('\t')
                    gene = split_data[0]
                    split_cell_data = split_data[1:]
                    idxs = [idx for idx, c in enumerate(split_cell_data) if float(c) != 0]
                    for idx in idxs:
                        outputs['cell_to_genes'].write(json.dumps({
                            'n1_type': 'cell',
                            'n2_type': 'gene',
                            'n1_value': '{};{}'.format(dataset, cells[idx]),
                            'n2_value': gene,
                            'n1': get_node_id('cell', (dataset, cells[idx])),
                            'n2': get_node_id('gene', gene),
                            'value': float(split_cell_data[idx]),
                            'value_field': 'norm_count'
                        }) + '\n')
                        outputs['gene_to_cells'].write(json.dumps({
                            'n1_type': 'gene',
                            'n2_type': 'cell',
                            'n1_value': gene,
                            'n2_value': '{};{}'.format(dataset, cells[idx]),
                            'n1': get_node_id('gene', gene),
                            'n2': get_node_id('cell', (dataset, cells[idx])),
                            'value': float(split_cell_data[idx]),
                            'value_field': 'norm_count'
                        }) + '\n')
            os.remove('inputs/norm_counts.sample.tsv.gz')
    for f_out in outputs.values():
        f_out.close()


def get_attributes(dataset, cell_type, model):
    file_in = f'{s3_in}/out/single_cell/regression/{dataset}/{cell_type}/{model}/donor_factor.regression.results.tsv'
    attributes = set()
    if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
        with open('inputs/donor_factor.regression.results.tsv', 'r') as f:
            _ = f.readline()  # header
            for line in f:
                attributes |= {line.strip().split('\t')[1]}
        os.remove('inputs/donor_factor.regression.results.tsv')
    return list(attributes)


def process_metadata(dataset_celltype_models):
    outputs = {
        'cell_to_donors': gzip.open('outputs/cell_to_donors.json.gz', 'wt'),
        'donor_to_cells': gzip.open('outputs/donor_to_cells.json.gz', 'wt'),
        'donor_to_attributes': gzip.open('outputs/donor_to_attributes.json.gz', 'wt'),
        'attribute_to_donors': gzip.open('outputs/attribute_to_donors.json.gz', 'wt')
    }
    for dataset, cell_type, model in dataset_celltype_models:
        file_in = f'{s3_in}/out/single_cell/staging/downsample/{dataset}/{cell_type}/sample_metadata.sample.tsv.gz'
        if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
            subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
            attributes = get_attributes(dataset, cell_type, model)
            with gzip.open('inputs/sample_metadata.sample.tsv.gz', 'rt') as f:
                header = f.readline().strip().split('\t')
                for line in f:
                    line_dict = dict(zip(header, line.strip().split('\t')))
                    outputs['cell_to_donors'].write(json.dumps({
                        'n1_type': 'cell',
                        'n2_type': 'donor',
                        'n1_value': '{};{}'.format(dataset, line_dict['ID']),
                        'n2_value': '{};{}'.format(dataset, line_dict['donor_id']),
                        'n1': get_node_id('cell', (dataset, line_dict['ID'])),
                        'n2': get_node_id('donor', (dataset, line_dict['donor_id'])),
                        'value': 1,
                        'value_field': 'identity'
                    }) + '\n')
                    outputs['donor_to_cells'].write(json.dumps({
                        'n1_type': 'donor',
                        'n2_type': 'cell',
                        'n1_value': '{};{}'.format(dataset, line_dict['donor_id']),
                        'n2_value': '{};{}'.format(dataset, line_dict['ID']),
                        'n1': get_node_id('donor', (dataset, line_dict['donor_id'])),
                        'n2': get_node_id('cell', (dataset, line_dict['ID'])),
                        'value': 1,
                        'value_field': 'identity'
                    }) + '\n')
                    for attribute in attributes:
                        outputs['donor_to_attributes'].write(json.dumps({
                            'n1_type': 'donor',
                            'n2_type': 'attribute',
                            'n1_value': '{};{}'.format(dataset, line_dict['donor_id']),
                            'n2_value': '{};{}'.format(dataset, attribute),
                            'n1': get_node_id('donor', (dataset, line_dict['donor_id'])),
                            'n2': get_node_id('attribute', (dataset, attribute)),
                            'value': float(line_dict[attribute]),
                            'value_field': 'attribute_value'
                        }) + '\n')
                        outputs['attribute_to_donors'].write(json.dumps({
                            'n1_type': 'attribute',
                            'n2_type': 'donor',
                            'n1_value': '{};{}'.format(dataset, attribute),
                            'n2_value': '{};{}'.format(dataset, line_dict['donor_id']),
                            'n1': get_node_id('attribute', (dataset, attribute)),
                            'n2': get_node_id('donor', (dataset, line_dict['donor_id'])),
                            'value': float(line_dict[attribute]),
                            'value_field': 'attribute_value'
                        }) + '\n')
            os.remove('inputs/sample_metadata.sample.tsv.gz')
    for f_out in outputs.values():
        f_out.close()


def process_gene(dataset_celltype_models):
    outputs = {
        'factor_to_genes': gzip.open('outputs/factor_to_genes.json.gz', 'wt'),
        'gene_to_factors': gzip.open('outputs/gene_to_factors.json.gz', 'wt')
    }
    for dataset, cell_type, model in dataset_celltype_models:
        file_in = f'{s3_in}/out/single_cell/staging/factor_matrix/{dataset}/{cell_type}/{model}/factor_matrix_gene_loadings.tsv'
        if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
            subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
            with open('inputs/factor_matrix_gene_loadings.tsv', 'r') as f:
                factors = f.readline().strip().split('\t')[1:]
                for line in f:
                    split_data = line.strip().split('\t')
                    gene = split_data[0]
                    split_factor_data = split_data[1:]
                    # TODO: filter to 0.01 * max loading for factor
                    for factor, factor_value in zip(factors, split_factor_data):
                        outputs['factor_to_genes'].write(json.dumps({
                            'n1_type': 'factor',
                            'n2_type': 'gene',
                            'n1_value': '{};{};{};{}'.format(dataset, cell_type, model, factor),
                            'n2_value': gene,
                            'n1': get_node_id('factor', (dataset, cell_type, model, factor)),
                            'n2': get_node_id('gene', gene),
                            'value': float(factor_value),
                            'value_field': 'factor_value'
                        }) + '\n')
                        outputs['gene_to_factors'].write(json.dumps({
                            'n1_type': 'gene',
                            'n2_type': 'factor',
                            'n1_value': gene,
                            'n2_value': '{};{};{};{}'.format(dataset, cell_type, model, factor),
                            'n1': get_node_id('gene', gene),
                            'n2': get_node_id('factor', (dataset, cell_type, model, factor)),
                            'value': float(factor_value),
                            'value_field': 'factor_value'
                        }) + '\n')
            os.remove('inputs/factor_matrix_gene_loadings.tsv')
    for f_out in outputs.values():
        f_out.close()


def process_cell(dataset_celltype_models):
    outputs = {
        'factor_to_cells': gzip.open('outputs/factor_to_cells.json.gz', 'wt'),
        'cell_to_factors': gzip.open('outputs/cell_to_factors.json.gz', 'wt')
    }
    for dataset, cell_type, model in dataset_celltype_models:
        file_in = f'{s3_in}/out/single_cell/staging/factor_matrix/{dataset}/{cell_type}/{model}/factor_matrix_cell_loadings.tsv'
        if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
            subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
            with open('inputs/factor_matrix_cell_loadings.tsv', 'r') as f:
                factors = f.readline().strip().split('\t')[2:]
                for line in f:
                    split_data = line.strip().split('\t')
                    cell = split_data[0]
                    split_factor_data = split_data[2:]
                    for factor, factor_value in zip(factors, split_factor_data):
                        outputs['factor_to_cells'].write(json.dumps({
                            'n1_type': 'factor',
                            'n2_type': 'cell',
                            'n1_value': '{};{};{};{}'.format(dataset, cell_type, model, factor),
                            'n2_value': '{};{}'.format(dataset, cell),
                            'n1': get_node_id('factor', (dataset, cell_type, model, factor)),
                            'n2': get_node_id('cell', (dataset, cell)),
                            'value': float(factor_value),
                            'value_field': 'factor_value'
                        }) + '\n')
                        outputs['cell_to_factors'].write(json.dumps({
                            'n1_type': 'cell',
                            'n2_type': 'factor',
                            'n1_value': '{};{}'.format(dataset, cell),
                            'n2_value': '{};{};{};{}'.format(dataset, cell_type, model, factor),
                            'n2': get_node_id('cell', (dataset, cell)),
                            'n1': get_node_id('factor', (dataset, cell_type, model, factor)),
                            'value': float(factor_value),
                            'value_field': 'factor_value'
                        }) + '\n')
            os.remove('inputs/factor_matrix_cell_loadings.tsv')
    for f_out in outputs.values():
        f_out.close()


def process_phewas(dataset_cell_type_models):
    outputs = {
        'factor_to_traits': gzip.open('outputs/factor_to_traits.json.gz', 'wt'),
        'trait_to_factors': gzip.open('outputs/trait_to_factors.json.gz', 'wt')
    }
    for dataset, cell_type, model in dataset_cell_type_models:
        file_in = f'{s3_in}/out/single_cell/staging/factor_phewas/{dataset}/{cell_type}/{model}/phewas_gene_loadings.txt'
        if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
            subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
            with open('inputs/phewas_gene_loadings.txt', 'r') as f:
                header = f.readline().strip().split('\t')
                for line in f:
                    json_line = dict(zip(header, line.strip().split('\t')))
                    p = max([float(json_line['P']), float(json_line['P_robust'])])
                    if p < 0.5:
                        outputs['factor_to_traits'].write(json.dumps({
                            'n1_type': 'factor',
                            'n2_type': 'trait',
                            'n1': get_node_id('factor', (dataset, cell_type, model, json_line['Factor'])),
                            'n2': get_node_id('trait', json_line['Pheno']),
                            'n1_value': '{};{};{};{}'.format(dataset, cell_type, model, json_line['Factor']),
                            'n2_value': json_line['Pheno'],
                            'value': -math.log10(p) if p > 0 else np.nextafter(0, 1),
                            'value_field': '-log10(p)'
                        }) + '\n')
                        outputs['trait_to_factors'].write(json.dumps({
                            'n1_type': 'trait',
                            'n2_type': 'factor',
                            'n1': get_node_id('trait', json_line['Pheno']),
                            'n2': get_node_id('factor', (dataset, cell_type, model, json_line['Factor'])),
                            'n1_value': json_line['Pheno'],
                            'n2_value': '{};{};{};{}'.format(dataset, cell_type, model, json_line['Factor']),
                            'value': -math.log10(p) if p > 0 else np.nextafter(0, 1),
                            'value_field': '-log10(p)'
                        }) + '\n')
            os.remove('inputs/phewas_gene_loadings.txt')
    for f_out in outputs.values():
        f_out.close()


def process_regression(dataset_celltype_models):
    outputs = {
        'factor_to_attributes': gzip.open('outputs/factor_to_attributes.json.gz', 'wt'),
        'attribute_to_factors': gzip.open('outputs/attribute_to_factors.json.gz', 'wt')
    }
    for dataset, cell_type, model in dataset_celltype_models:
        file_in = f'{s3_in}/out/single_cell/regression/{dataset}/{cell_type}/{model}/donor_factor.regression.results.tsv'
        if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
            subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
            with open('inputs/donor_factor.regression.results.tsv', 'r') as f:
                _ = f.readline().strip().split('\t')
                for line in f:
                    factor, attribute, coef, _ = line.strip().split('\t')
                    outputs['factor_to_attributes'].write(json.dumps({
                        'n1_type': 'factor',
                        'n2_type': 'attribute',
                        'n1': get_node_id('factor', (dataset, cell_type, model, factor)),
                        'n2': get_node_id('attribute', (dataset, attribute)),
                        'n1_value': '{};{};{};{}'.format(dataset, cell_type, model, factor),
                        'n2_value': '{};{}'.format(dataset, attribute),
                        'value': float(coef),
                        'value_field': 'coef'
                    }) + '\n')
                    outputs['attribute_to_factors'].write(json.dumps({
                        'n1_type': 'attribute',
                        'n2_type': 'factor',
                        'n1': get_node_id('attribute', (dataset, attribute)),
                        'n2': get_node_id('factor', (dataset, cell_type, model, factor)),
                        'n1_value': '{};{}'.format(dataset, attribute),
                        'n2_value': '{};{};{};{}'.format(dataset, cell_type, model, factor),
                        'value': float(coef),
                        'value_field': 'coef'
                    }) + '\n')
            os.remove('inputs/donor_factor.regression.results.tsv')
    for f_out in outputs.values():
        f_out.close()


def process_pigean(dataset_celltype_models):
    outputs = {
        'factor_to_gene_sets': gzip.open('outputs/factor_to_gene_sets.json.gz', 'wt'),
        'gene_set_to_factors': gzip.open('outputs/gene_set_to_factors.json.gz', 'wt')
    }
    for dataset, cell_type, model in dataset_celltype_models:
        file_in = f'{s3_in}/out/single_cell/pigean/{dataset}/{cell_type}/{model}/pigean.gene_sets.tsv'
        if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
            subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
            with open('inputs/pigean.gene_sets.tsv', 'r') as f:
                _ = f.readline().strip().split('\t')
                for line in f:
                    factor_num, gene_set, beta = line.strip().split('\t')
                    if float(beta) > 0.01:
                        outputs['factor_to_gene_sets'].write(json.dumps({
                            'n1_type': 'factor',
                            'n2_type': 'gene_set',
                            'n1': get_node_id('factor', (dataset, cell_type, model, 'Factor_{}'.format(factor_num))),
                            'n2': get_node_id('geneset', gene_set),
                            'n1_value': '{};{};{};{}'.format(dataset, cell_type, model, 'Factor_{}'.format(factor_num)),
                            'n2_value': gene_set,
                            'value': float(beta),
                            'value_field': 'beta'
                        }) + '\n')
                        outputs['gene_set_to_factors'].write(json.dumps({
                            'n1_type': 'gene_set',
                            'n2_type': 'factor',
                            'n1': get_node_id('geneset', gene_set),
                            'n2': get_node_id('factor', (dataset, cell_type, model, 'Factor_{}'.format(factor_num))),
                            'n1_value': gene_set,
                            'n2_value': '{};{};{};{}'.format(dataset, cell_type, model, 'Factor_{}'.format(factor_num)),
                            'value': float(beta),
                            'value_field': 'beta'
                        }) + '\n')
            os.remove('inputs/pigean.gene_sets.tsv')
    for f_out in outputs.values():
        f_out.close()


def get_function(run_type):
    return {
        'norm': process_norm,
        'metadata': process_metadata,
        'gene': process_gene,
        'cell': process_cell,
        'phewas': process_phewas,
        'regression': process_regression,
        'pigean': process_pigean
    }[run_type]


def upload():
    path = f'{s3_out}/out/single_cell/graph/'
    subprocess.check_call(['aws', 's3', 'cp', 'outputs/', f'{path}', '--recursive'])


def main():
    dataset_celltype_models = get_dataset_celltype_models()
    for run_type in ['gene', 'phewas', 'pigean']:
        os.makedirs('outputs', exist_ok=True)
        get_function(run_type)(dataset_celltype_models)
        upload()
        shutil.rmtree('outputs')


if __name__ == '__main__':
    main()
