#!/usr/bin/python3
import glob
import os
import re
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']
trait_uri_key = 'MAPPED_TRAIT_URI'


def download_gwas_catalog():
    subprocess.check_call('aws s3 cp s3://dig-analysis-bin/gwas_catalog/gwas_catalog_associations.tsv ./gwas_data/', shell=True)
    subprocess.check_call('aws s3 cp s3://dig-analysis-bin/gwas_catalog/gwas_catalog_metadata.tsv ./gwas_data/', shell=True)


def get_count():
    count = {}
    with open('gwas_data/gwas_catalog_associations.tsv', 'r') as f:
        header = f.readline().strip().split('\t')
        for line in f:
            data_dict = dict(zip(header, line.strip().split('\t')))
            trait_uri = data_dict[trait_uri_key]
            if ',' not in trait_uri:
                if trait_uri not in count:
                    count[trait_uri] = 0
                count[trait_uri] += 1
    return count


def save_names(name_map):
    with open('code_to_name.tsv', 'w') as f:
        for code_id in sorted(name_map):
            f.write(f'Orphanet_{code_id}\t{name_map[code_id]}\n')
    subprocess.check_call('aws s3 cp code_to_name.tsv s3://dig-analysis-bin/gwas_catalog/', shell=True)
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
    #download_gwas_catalog()
    count = get_count()
    print(len(count))
    print(len([a for a in count if 'EFO' in a and count[a] >= 10]))
    print(len([a for a in count if count[a] >= 10]))


if __name__ == '__main__':
    run()
