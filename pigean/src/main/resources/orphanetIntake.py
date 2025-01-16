#!/usr/bin/python3
from bs4 import BeautifulSoup
import glob
import os
import re
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

def assign_probabilities(association_text):
    if "Disease-causing germline mutation(s) in" in association_text:
        return 0.95
    elif "Disease-causing germline mutation(s) (gain of function) in" in association_text:
        return 0.90
    elif "Disease-causing germline mutation(s) (loss of function) in" in association_text:
        return 0.90
    elif "Disease-causing somatic mutation(s) in" in association_text:
        return 0.85
    elif "Major susceptibility factor in" in association_text:
        return 0.75
    elif "Part of a fusion gene in" in association_text:
        return 0.65
    elif "Role in the phenotype of" in association_text:
        return 0.60
    elif "Candidate gene tested in" in association_text:
        return 0.50
    elif "Biomarker tested in" in association_text:
        return 0.40
    elif "Modifying germline mutation in" in association_text:
        return 0.30
    else:
        return None  # Default if text doesn't match anything

def download_orphanet_xml():
    subprocess.check_call('aws s3 cp s3://dig-analysis-bin/orphanet/classifications/ ./classifications/ --recursive', shell=True)
    subprocess.check_call('aws s3 cp s3://dig-analysis-bin/orphanet/genes/ ./genes/ --recursive', shell=True)


def process_name(s):
    return s.replace(',', ';').encode('utf-8').decode('ascii', errors='ignore')


def get_gene_data():
    with open('genes/genes.xml', 'r', encoding='ISO-8859-1') as f:
        soup = BeautifulSoup(f, features='xml')

    gene_map = {}
    name_map = {}
    for code in soup.find_all('OrphaCode'):
        code_id = int(code.text)
        name = process_name(code.find_next('Name').text)
        if 'NON_RARE_IN_EUROPE' not in name:
            gene_list = code.find_next('DisorderGeneAssociationList')
            genes = {}
            for gene in gene_list.find_all('Gene'):
                gene_name = gene.find_next('Symbol').text
                gene_prob = assign_probabilities(gene.find_next('DisorderGeneAssociationType').text)
                if gene_name not in genes or gene_prob > genes[gene_name]:
                    genes[gene_name] = gene_prob
            name_map[code_id] = name
            gene_map[code_id] = genes
    return name_map, gene_map


def add_classifications(name_map, gene_map):
    disorder_map = {k: v for k, v in gene_map.items()}
    for file in glob.glob('classifications/*.xml'):
        with open(file, 'r', encoding='ISO-8859-1') as f:
            soup = BeautifulSoup(f, features='xml')

        nodes = soup.find_all('ClassificationNode')
        for node in nodes:
            disorder = node.find('Disorder')
            disorder_name = process_name(disorder.find('Name').text)
            disorder_code = int(disorder.find('OrphaCode').text)
            classification = node.find('ClassificationNodeChildList')
            if disorder_code not in disorder_map:
                disorder_map[disorder_code] = {}
            for classification_code in classification.find_all('OrphaCode'):
                code_id = int(classification_code.text)
                if code_id in gene_map:
                    for gene, prob in gene_map[code_id].items():
                        if gene not in disorder_map[disorder_code] or disorder_map[disorder_code][gene] < prob:
                            disorder_map[disorder_code][gene] = prob
            name_map[disorder_code] = disorder_name
    return name_map, disorder_map


def filter_by_size(name_map, gene_map):
    filtered_name_map = {}
    filtered_gene_map = {}
    for code_id, genes in gene_map.items():
        if len(genes) >= 2:
            filtered_name_map[code_id] = name_map[code_id]
            filtered_gene_map[code_id] = gene_map[code_id]
    return filtered_name_map, filtered_gene_map


def check_file(path):
    return subprocess.call(f'aws s3 ls {path}', shell=True)


def download_all_gene_lists():
    path = f'{s3_in}/out/pigean/inputs/gene_lists/rare_v2/'
    os.makedirs('gene_lists', exist_ok=True)
    if check_file(path) == 0:
        subprocess.check_call(f'aws s3 cp {path} ./gene_lists/ --recursive', shell=True)


def get_old_gene_map():
    old_gene_map = {}
    for file in glob.glob('gene_lists/*'):
        code_id = int(re.findall('gene_lists/Orphanet_([0-9]*)', file)[0])
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
            f.write(f'Orphanet_{code_id}\t{name_map[code_id]}\n')
    subprocess.check_call('aws s3 cp code_to_name.tsv s3://dig-analysis-bin/orphanet/', shell=True)
    os.remove('code_to_name.tsv')


def save_data(name_map, gene_map, new_or_altered_ids):
    save_names(name_map)
    os.makedirs('output', exist_ok=True)
    for new_or_altered_id in new_or_altered_ids:
        file_name = f'Orphanet_{new_or_altered_id}'
        os.makedirs(f'output/{file_name}', exist_ok=True)
        with open(f'output/{file_name}/gene_list.tsv', 'w') as f:
            for gene, prob in gene_map[new_or_altered_id].items():
                f.write(f'{gene}\t{prob}\n')
        subprocess.check_call(f'touch output/{file_name}/_SUCCESS', shell=True)
    subprocess.check_call(f'aws s3 cp output/ {s3_out}/out/pigean/inputs/gene_lists/rare_v2/ --recursive', shell=True)
    shutil.rmtree('output')


def run():
    download_orphanet_xml()
    name_map, gene_map = get_gene_data()
    name_map, gene_map = add_classifications(name_map, gene_map)
    name_map, gene_map = filter_by_size(name_map, gene_map)

    download_all_gene_lists()
    old_gene_map = get_old_gene_map()

    new_or_altered_ids = get_new_or_altered_ids(gene_map, old_gene_map)

    save_data(name_map, gene_map, new_or_altered_ids)
    shutil.rmtree('classifications')
    shutil.rmtree('genes')
    shutil.rmtree('gene_lists')

if __name__ == '__main__':
    run()
