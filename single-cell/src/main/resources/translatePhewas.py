#!/usr/bin/python3
import argparse
import json
import os
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_data(dataset, cell_type, model):
    file_path = f'{s3_in}/out/single_cell/staging/factor_phewas/{dataset}/{cell_type}/{model}/phewas_gene_loadings.txt'
    subprocess.check_call(['aws', 's3', 'cp', file_path, 'inputs/phewas_gene_loadings.txt'])


def upload_data(dataset, cell_type, model):
    file_path = f'{s3_out}/out/single_cell/phewas/{dataset}/{cell_type}/{model}/'
    subprocess.check_call(['aws', 's3', 'cp', 'outputs/phewas.json', file_path])
    success(file_path)


def make_option(value):
    return value if value != 'NA' else 'null'

def get_trait_display_map():
    trait_display_map = {}
    #  TODO: Something more consistent and permanent, this is from the cfde bioindex, but I moved it to bin
    file = 's3://dig-analysis-bin/pigean/misc/trait_data_cfde.json'
    subprocess.check_call(['aws', 's3', 'cp', file, 'inputs/'])
    with open('inputs/trait_data_cfde.json', 'r') as f:
        for line in f:
            json_line = json.loads(line.strip())
            trait_display_map[json_line['phenotype']] = (json_line['trait_group'], json_line['phenotype_name'])
    return trait_display_map


def translate_phewas_line(json_line, trait_map, dataset, cell_type, model):
    pValue = make_option(json_line["P"])
    if pValue is not None:
        trait = json_line['Pheno']
        if trait in trait_map:
            return f'{{"factor": "{json_line["Factor"]}", ' \
                   f'"trait": "{trait}", ' \
                   f'"trait_group": "{trait_map[trait][0]}", ' \
                   f'"trait_name": "{trait_map[trait][1]}", ' \
                   f'"pValue": {pValue}, ' \
                   f'"dataset": "{dataset}", ' \
                   f'"cell_type": "{cell_type}", ' \
                   f'"model": "{model}"}}\n'


def translate_phewas(dataset, cell_type, model):
    trait_map = get_trait_display_map()
    with open('outputs/phewas.json', 'w') as f_out:
        with open('inputs/phewas_gene_loadings.txt', 'r') as f_in:
            header = f_in.readline().strip().split('\t')
            for line in f_in:
                json_line = dict(zip(header, line.strip().split('\t')))
                str_line = translate_phewas_line(json_line, trait_map, dataset, cell_type, model)
                if str_line is not None:
                    f_out.write(str_line)


def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default=None, required=True, type=str)
    parser.add_argument('--cell-type', default=None, required=True, type=str)
    parser.add_argument('--model', default=None, required=True, type=str)
    args = parser.parse_args()

    download_data(args.dataset, args.cell_type, args.model)
    os.makedirs('outputs', exist_ok=True)
    translate_phewas(args.dataset, args.cell_type, args.model)
    upload_data(args.dataset, args.cell_type, args.model)
    shutil.rmtree('inputs')
    shutil.rmtree('outputs')

if __name__ == '__main__':
    main()
