#!/usr/bin/python3
import glob
import os
import re
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def process_name(s):
    return s.replace(',', ';').replace('"', '\'').encode('utf-8').decode('ascii', errors='ignore')


def download_and_parse_hpo():
    subprocess.check_call('aws s3 cp s3://dig-analysis-bin/hpo/phenotype_to_genes.txt .', shell=True)
    gene_map = {}
    name_map = {}
    with open('phenotype_to_genes.txt', 'r') as f:
        header = f.readline().strip().split('\t')
        for line in f:
            line_dict = dict(zip(header, line.strip().split('\t')))
            id = int(line_dict['hpo_id'].strip().split(':')[1])
            if id not in gene_map:
                gene_map[id] = {}
                name_map[id] = process_name(line_dict['hpo_name'])
            gene_map[id][line_dict['gene_symbol']] = 0.95
    return name_map, gene_map


def filter_by_size(name_map, gene_map):
    filtered_name_map = {}
    filtered_gene_map = {}
    for code_id, genes in gene_map.items():
        if 2 <= len(genes) <= 1000:
            filtered_name_map[code_id] = name_map[code_id]
            filtered_gene_map[code_id] = gene_map[code_id]
    return filtered_name_map, filtered_gene_map


def check_file(path):
    return subprocess.call(f'aws s3 ls {path}', shell=True)


def download_all_gene_lists():
    path = f'{s3_in}/out/pigean/inputs/gene_lists/hpo/'
    os.makedirs('gene_lists', exist_ok=True)
    if check_file(path) == 0:
        subprocess.check_call(f'aws s3 cp {path} ./gene_lists/ --recursive', shell=True)


def get_old_gene_map():
    old_gene_map = {}
    for file in glob.glob('gene_lists/*'):
        code_id = int(re.findall('gene_lists/HP_([0-9]*)', file)[0])
        genes= {}
        with open(f'{file}/gene_list.tsv', 'r') as f:
            for line in f:
                gene, prob = line.strip().split('\t')
                genes[gene] = float(prob)
        old_gene_map[code_id] = genes
    return old_gene_map


def get_new_or_altered_ids(gene_map, old_gene_map):
    new_or_altered_ids = []
    for code_id in gene_map:
        if code_id not in old_gene_map or gene_map[code_id] != old_gene_map[code_id]:
            new_or_altered_ids.append(code_id)
    return new_or_altered_ids


def save_names(name_map):
    with open('code_to_name.tsv', 'w') as f:
        for code_id in sorted(name_map):
            f.write(f'HP_{str(code_id).zfill(7)}\t{name_map[code_id]}\n')
    subprocess.check_call('aws s3 cp code_to_name.tsv s3://dig-analysis-bin/hpo/', shell=True)
    os.remove('code_to_name.tsv')


def save_data(name_map, gene_map, new_or_altered_ids):
    save_names(name_map)
    os.makedirs('output', exist_ok=True)
    for new_or_altered_id in new_or_altered_ids:
        file_name = f'HP_{str(new_or_altered_id).zfill(7)}'
        os.makedirs(f'output/{file_name}', exist_ok=True)
        with open(f'output/{file_name}/gene_list.tsv', 'w') as f:
            for gene, prob in gene_map[new_or_altered_id].items():
                f.write(f'{gene}\t{prob}\n')
        subprocess.check_call(f'touch output/{file_name}/_SUCCESS', shell=True)
    subprocess.check_call(f'aws s3 cp output/ {s3_out}/out/pigean/inputs/gene_lists/hpo/ --recursive', shell=True)
    shutil.rmtree('output')


def run():
    name_map, gene_map = download_and_parse_hpo()
    name_map, gene_map = filter_by_size(name_map, gene_map)

    download_all_gene_lists()
    old_gene_map = get_old_gene_map()

    new_or_altered_ids = get_new_or_altered_ids(gene_map, old_gene_map)

    save_data(name_map, gene_map, new_or_altered_ids)
    os.remove('phenotype_to_genes.txt')
    shutil.rmtree('gene_lists')

if __name__ == '__main__':
    run()
