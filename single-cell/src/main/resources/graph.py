#!/usr/bin/python3
import argparse
import gzip
import json
import math
import numpy as np
import os
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def process_norm(dataset, cell_type, model):
    file_in = f'{s3_in}/out/single_cell/staging/downsample/{dataset}/{cell_type}/norm_counts.sample.tsv.gz'
    if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
        outputs = {
            'cell_to_genes': gzip.open('outputs/cell_to_genes.json.gz', 'wt'),
            'gene_to_cells': gzip.open('outputs/gene_to_cells.json.gz', 'wt')
        }
        with gzip.open('inputs/norm_counts.sample.tsv.gz', 'rt') as f:
            cells = f.readline().strip().split('\t')[1:]
            for line in f:
                split_data = line.strip().split('\t')
                gene = split_data[0]
                split_cell_data = split_data[1:]
                idxs = [idx for idx, c in enumerate(split_cell_data) if float(c) != 0]
                for idx in idxs:
                    outputs['cell_to_genes'].write(json.dumps({
                        'dataset': dataset,
                        'cell_type': cell_type,
                        'model': model,
                        'n1_type': 'cell',
                        'n2_type': 'gene',
                        'n1': cells[idx],
                        'n2': gene,
                        'value': float(split_cell_data[idx])
                    }) + '\n')
                    outputs['gene_to_cells'].write(json.dumps({
                        'dataset': dataset,
                        'cell_type': cell_type,
                        'model': model,
                        'n1_type': 'gene',
                        'n2_type': 'cell',
                        'n1': gene,
                        'n2': cells[idx],
                        'value': float(split_cell_data[idx])
                    }) + '\n')
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
    return list(attributes)


def process_metadata(dataset, cell_type, model):
    file_in = f'{s3_in}/out/single_cell/staging/downsample/{dataset}/{cell_type}/sample_metadata.sample.tsv.gz'
    if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
        outputs = {
            'cell_to_donors': gzip.open('outputs/cell_to_donors.json.gz', 'wt'),
            'donor_to_cells': gzip.open('outputs/donor_to_cells.json.gz', 'wt'),
            'donor_to_attributes': gzip.open('outputs/donor_to_attributes.json.gz', 'wt'),
            'attribute_to_donors': gzip.open('outputs/attribute_to_donors.json.gz', 'wt')
        }
        attributes = get_attributes(dataset, cell_type, model)
        with gzip.open('inputs/sample_metadata.sample.tsv.gz', 'rt') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                line_dict = dict(zip(header, line.strip().split('\t')))
                outputs['cell_to_donors'].write(json.dumps({
                    'dataset': dataset,
                    'cell_type': cell_type,
                    'model': model,
                    'n1_type': 'cell',
                    'n2_type': 'donor',
                    'n1': line_dict['ID'],
                    'n2': line_dict['donor_id'],
                    'value': 1
                }) + '\n')
                outputs['donor_to_cells'].write(json.dumps({
                    'dataset': dataset,
                    'cell_type': cell_type,
                    'model': model,
                    'n1_type': 'donor',
                    'n2_type': 'cell',
                    'n1': line_dict['donor_id'],
                    'n2': line_dict['ID'],
                    'value': 1
                }) + '\n')
                for attribute in attributes:
                    outputs['donor_to_attributes'].write(json.dumps({
                        'dataset': dataset,
                        'cell_type': cell_type,
                        'model': model,
                        'n1_type': 'donor',
                        'n2_type': 'attribute',
                        'n1': line_dict['donor_id'],
                        'n2': attribute,
                        'value': float(line_dict[attribute])
                    }) + '\n')
                    outputs['attribute_to_donors'].write(json.dumps({
                        'dataset': dataset,
                        'cell_type': cell_type,
                        'model': model,
                        'n1_type': 'attribute',
                        'n2_type': 'donor',
                        'n1': attribute,
                        'n2': line_dict['donor_id'],
                        'value': float(line_dict[attribute])
                    }) + '\n')
        for f_out in outputs.values():
            f_out.close()


def process_gene(dataset, cell_type, model):
    file_in = f'{s3_in}/out/single_cell/staging/factor_matrix/{dataset}/{cell_type}/{model}/factor_matrix_gene_loadings.tsv'
    if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
        outputs = {
            'factor_to_genes': gzip.open('outputs/factor_to_genes.json.gz', 'wt'),
            'gene_to_factors': gzip.open('outputs/gene_to_factors.json.gz', 'wt')
        }
        with open('inputs/factor_matrix_gene_loadings.tsv', 'r') as f:
            factors = f.readline().strip().split('\t')[1:]
            for line in f:
                split_data = line.strip().split('\t')
                gene = split_data[0]
                split_factor_data = split_data[1:]
                for factor, factor_value in zip(factors, split_factor_data):
                    outputs['factor_to_genes'].write(json.dumps({
                        'dataset': dataset,
                        'cell_type': cell_type,
                        'model': model,
                        'n1_type': 'factor',
                        'n2_type': 'gene',
                        'n1': factor,
                        'n2': gene,
                        'value': float(factor_value)
                    }) + '\n')
                    outputs['gene_to_factors'].write(json.dumps({
                        'dataset': dataset,
                        'cell_type': cell_type,
                        'model': model,
                        'n1_type': 'gene',
                        'n2_type': 'factor',
                        'n1': gene,
                        'n2': factor,
                        'value': float(factor_value)
                    }) + '\n')
        for f_out in outputs.values():
            f_out.close()


def process_cell(dataset, cell_type, model):
    file_in = f'{s3_in}/out/single_cell/staging/factor_matrix/{dataset}/{cell_type}/{model}/factor_matrix_cell_loadings.tsv'
    if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
        outputs = {
            'factor_to_cells': gzip.open('outputs/factor_to_cells.json.gz', 'wt'),
            'cell_to_factors': gzip.open('outputs/cell_to_factors.json.gz', 'wt')
        }
        with open('inputs/factor_matrix_cell_loadings.tsv', 'r') as f:
            factors = f.readline().strip().split('\t')[2:]
            for line in f:
                split_data = line.strip().split('\t')
                cell = split_data[0]
                split_factor_data = split_data[2:]
                for factor, factor_value in zip(factors, split_factor_data):
                    outputs['factor_to_cells'].write(json.dumps({
                        'dataset': dataset,
                        'cell_type': cell_type,
                        'model': model,
                        'n1_type': 'factor',
                        'n2_type': 'cell',
                        'n1': factor,
                        'n2': cell,
                        'value': float(factor_value)
                    }) + '\n')
                    outputs['cell_to_factors'].write(json.dumps({
                        'dataset': dataset,
                        'cell_type': cell_type,
                        'model': model,
                        'n1_type': 'cell',
                        'n2_type': 'factor',
                        'n1': cell,
                        'n2': factor,
                        'value': float(factor_value)
                    }) + '\n')
        for f_out in outputs.values():
            f_out.close()


def process_phewas(dataset, cell_type, model):
    file_in = f'{s3_in}/out/single_cell/staging/factor_phewas/{dataset}/{cell_type}/{model}/phewas_gene_loadings.txt'
    if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
        outputs = {
            'factor_to_traits': gzip.open('outputs/factor_to_traits.json.gz', 'wt'),
            'trait_to_factors': gzip.open('outputs/trait_to_factors.json.gz', 'wt')
        }
        with open('inputs/phewas_gene_loadings.txt', 'r') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                json_line = dict(zip(header, line.strip().split('\t')))
                p = max([float(json_line['P']), float(json_line['P_robust'])])
                if p < 0.5:
                    outputs['factor_to_traits'].write(json.dumps({
                        'dataset': dataset,
                        'cell_type': cell_type,
                        'model': model,
                        'n1_type': 'factor',
                        'n2_type': 'trait',
                        'n1': json_line['Factor'],
                        'n2': json_line['Pheno'],
                        'value': -math.log10(p) if p > 0 else np.nextafter(0, 1)
                    }) + '\n')
                    outputs['trait_to_factors'].write(json.dumps({
                        'dataset': dataset,
                        'cell_type': cell_type,
                        'model': model,
                        'n1_type': 'trait',
                        'n2_type': 'factor',
                        'n1': json_line['Pheno'],
                        'n2': json_line['Factor'],
                        'value': -math.log10(p) if p > 0 else np.nextafter(0, 1)
                    }) + '\n')
        for f_out in outputs.values():
            f_out.close()


def process_regression(dataset, cell_type, model):
    file_in = f'{s3_in}/out/single_cell/regression/{dataset}/{cell_type}/{model}/donor_factor.regression.results.tsv'
    if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
        outputs = {
            'factor_to_attributes': gzip.open('outputs/factor_to_attributes.json.gz', 'wt'),
            'attribute_to_factors': gzip.open('outputs/attribute_to_factors.json.gz', 'wt')
        }
        with open('inputs/donor_factor.regression.results.tsv', 'r') as f:
            _ = f.readline().strip().split('\t')
            for line in f:
                factor, attribute, coef, _ = line.strip().split('\t')
                outputs['factor_to_attributes'].write(json.dumps({
                    'dataset': dataset,
                    'cell_type': cell_type,
                    'model': model,
                    'n1_type': 'factor',
                    'n2_type': 'attribute',
                    'n1': factor,
                    'n2': attribute,
                    'value': float(coef)
                }) + '\n')
                outputs['attribute_to_factors'].write(json.dumps({
                    'dataset': dataset,
                    'cell_type': cell_type,
                    'model': model,
                    'n1_type': 'attribute',
                    'n2_type': 'factor',
                    'n1': attribute,
                    'n2': factor,
                    'value': float(coef)
                }) + '\n')
        for f_out in outputs.values():
            f_out.close()


def process_pigean(dataset, cell_type, model):
    file_in = f'{s3_in}/out/single_cell/pigean/{dataset}/{cell_type}/{model}/pigean.gene_sets.tsv'
    if subprocess.call(['aws', 's3', 'ls', f'{file_in}']) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_in}', 'inputs/'])
        outputs = {
            'factor_to_gene_sets': gzip.open('outputs/factor_to_gene_sets.json.gz', 'wt'),
            'gene_set_to_factors': gzip.open('outputs/gene_set_to_factors.json.gz', 'wt')
        }
        with open('inputs/pigean.gene_sets.tsv', 'r') as f:
            _ = f.readline().strip().split('\t')
            for line in f:
                factor_num, gene_set, beta = line.strip().split('\t')
                # Will want to filter, but check on how
                if float(beta) > 0.01:
                    outputs['factor_to_gene_sets'].write(json.dumps({
                        'dataset': dataset,
                        'cell_type': cell_type,
                        'model': model,
                        'n1_type': 'factor',
                        'n2_type': 'gene_set',
                        'n1': 'Factor_{}'.format(factor_num),
                        'n2': gene_set,
                        'value': float(beta)
                    }) + '\n')
                    outputs['gene_set_to_factors'].write(json.dumps({
                        'dataset': dataset,
                        'cell_type': cell_type,
                        'model': model,
                        'n1_type': 'gene_set',
                        'n2_type': 'factor',
                        'n1': gene_set,
                        'n2': 'Factor_{}'.format(factor_num),
                        'value': float(beta)
                    }) + '\n')
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


def upload(dataset, cell_type, model):
    path = f'{s3_out}/out/single_cell/graph/{dataset}/{cell_type}/{model}'
    subprocess.check_call(['aws', 's3', 'cp', 'outputs/', f'{path}', '--recursive'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default=None, required=True, type=str,
                        help="Dataset name")
    parser.add_argument('--cell-type', default=None, required=True, type=str,
                        help="Cell Type")
    parser.add_argument('--model', default=None, required=True, type=str,
                        help="Model")
    parser.add_argument('--run-type', default=None, required=True, type=str,
                        help="Run Type")
    args = parser.parse_args()

    os.makedirs('inputs', exist_ok=True)
    os.makedirs('outputs', exist_ok=True)
    get_function(args.run_type)(args.dataset, args.cell_type, args.model)
    upload(args.dataset, args.cell_type, args.model)
    shutil.rmtree('outputs')
    shutil.rmtree('inputs')

if __name__ == '__main__':
    main()
