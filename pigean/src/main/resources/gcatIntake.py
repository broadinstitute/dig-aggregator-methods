#!/usr/bin/python3
import glob
import gzip
import os
import re
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_gwas_catalog():
    subprocess.check_call('aws s3 cp s3://dig-analysis-bin/gwas_catalog/gwas_catalog_associations.tsv ./gwas_data/', shell=True)
    subprocess.check_call('aws s3 cp s3://dig-analysis-bin/gwas_catalog/gwas_catalog_ancestry.tsv ./gwas_data/', shell=True)


def get_mapped_trait(data_dict):
    trait = data_dict['DISEASE/TRAIT'].strip().replace(',', ';').replace('"', '\'')
    mapped_trait = data_dict['MAPPED_TRAIT'].strip().replace(',', ';').replace('"', '\'')
    if mapped_trait == '':
        mapped_trait = trait
    return mapped_trait


def format_trait(trait):
    return 'gcat_trait_{}'.format(re.sub(r'[^a-zA-Z0-9_-]', '', trait.replace(' ', '_')))


def get_traits():
    count = {}
    with open('gwas_data/gwas_catalog_associations.tsv', 'r') as f:
        header = f.readline().strip().split('\t')
        for line in f:
            data_dict = dict(zip(header, line.strip().split('\t')))
            mapped_trait = get_mapped_trait(data_dict)
            mapped_trait_uri = data_dict['MAPPED_TRAIT_URI'].strip()
            if ',' not in mapped_trait_uri:
                if mapped_trait not in count:
                    count[mapped_trait] = 0
                count[mapped_trait] += 1
    return {format_trait(mapped_trait): mapped_trait for mapped_trait in count if count[mapped_trait] >= 10}


def get_sample_sizes():
    sample_sizes = {}
    with open('gwas_data/gwas_catalog_ancestry.tsv', 'r') as f:
        header = f.readline().strip().split('\t')
        for line in f:
            data_dict = dict(zip(header, line.strip().split('\t')))
            if data_dict['STAGE'] == 'initial' and len(data_dict['NUMBER OF INDIVDUALS']) > 0:
                sample_sizes[data_dict['STUDY ACCESSION']] = int(data_dict['NUMBER OF INDIVDUALS'])
    return sample_sizes


def get_trait_associations(traits):
    sample_sizes = get_sample_sizes()
    associations = {}
    with open('gwas_data/gwas_catalog_associations.tsv', 'r') as f:
        header = f.readline().strip().split('\t')
        for line in f:
            data_dict = dict(zip(header, line.strip().split('\t')))
            formatted_trait = format_trait(get_mapped_trait(data_dict))
            if formatted_trait in traits:
                if data_dict['STUDY ACCESSION'] in sample_sizes:
                    association = {
                        'SNPS': data_dict['SNPS'],
                        'P-VALUE': data_dict['P-VALUE'],
                        'N': str(sample_sizes[data_dict['STUDY ACCESSION']])
                    }
                    if formatted_trait not in associations:
                        associations[formatted_trait] = []
                    associations[formatted_trait].append(association)
    return associations


def download_rs_map():
    subprocess.check_call('aws s3 cp s3://dig-analysis-bin/gwas_catalog/rs_map.tsv.gz ./gwas_data/', shell=True)


def get_rs_map(rs_set):
    rs_map = {}
    with gzip.open(f'gwas_data/rs_map.tsv.gz', 'rt') as f:
        _ = f.readline()
        for line in f:
            rs_id, chromosome, position = line.strip().split('\t')
            if rs_id in rs_set:
                rs_map[rs_id] = (chromosome, position)
    return rs_map


def map_trait_associations(associations, rs_map):
    out = {}
    for trait, trait_associations in associations.items():
        out[trait] = []
        for association in trait_associations:
            if 'rs' in association['SNPS']:
                if association['SNPS'] in rs_map:
                    chromosome, position = rs_map[association['SNPS']]
                    out[trait].append({
                        'CHR_ID': chromosome,
                        'CHR_POS': position,
                        'P-VALUE': association['P-VALUE'],
                        'N': association['N']
                    })
    return out


def get_min_p_associations(associations):
    out = {}
    for trait, trait_associations in associations.items():
        grouped_associations = {}
        for association in trait_associations:
            key = (association['CHR_ID'], association['CHR_POS'])
            if key not in grouped_associations or association['P-VALUE'] < grouped_associations[key]['P-VALUE']:
                grouped_associations[key] = association
        out[trait] = list(grouped_associations.values())
    return out


def filter_associations(associations):
    out = {}
    for trait, trait_associations in associations.items():
        if len(trait_associations) >= 10:
            out[trait] = trait_associations
    return out


def check_file(path):
    return subprocess.call(f'aws s3 ls {path}', shell=True)


def download_all_sumstats():
    path = f'{s3_in}/out/pigean/inputs/sumstats/gcat_trait/'
    os.makedirs('sumstats', exist_ok=True)
    if check_file(path) == 0:
        subprocess.check_call(f'aws s3 cp {path} ./sumstats/ --recursive', shell=True)


def get_old_associations():
    old_associations = {}
    for file in glob.glob('sumstats/*'):
        trait = re.findall('sumstats/(.*)', file)[0]
        old_associations[trait] = []
        with gzip.open(f'{file}/pigean.sumstats.gz', 'rt') as f:
            _ = f.readline()
            for line in f:
                chromosome, position, pValue, n = line.strip().split('\t')
                old_associations[trait].append({
                    'CHR_ID': chromosome,
                    'CHR_POS': position,
                    'P-VALUE': pValue,
                    'N': n
                })
    return old_associations


def dictify(associations):
    return {(association['CHR_ID'], association['CHR_POS']): association for association in associations}


def get_new_or_altered_ids(associations, old_associations):
    new_or_altered_ids = []
    old_ids = []
    for trait in associations:
        if trait not in old_associations or dictify(associations[trait]) != dictify(old_associations[trait]):
            new_or_altered_ids.append(trait)
    for trait in old_associations:
        if trait not in associations:
            old_ids.append(trait)
    return new_or_altered_ids, old_ids


def save_names(traits):
    with open('code_to_name.tsv', 'w') as f:
        for formatted_trait in sorted(traits):
            f.write(f'{formatted_trait}\t{traits[formatted_trait]}\n')
    subprocess.check_call('aws s3 cp code_to_name.tsv s3://dig-analysis-bin/gwas_catalog/', shell=True)
    os.remove('code_to_name.tsv')


def save_data(traits, new_or_altered_ids, associations):
    save_names(traits)
    os.makedirs('output', exist_ok=True)
    for trait in traits:
        if trait in new_or_altered_ids:
            os.makedirs(f'output/{trait}', exist_ok=True)
            with gzip.open(f'output/{trait}/pigean.sumstats.gz', 'wt') as f:
                f.write('CHROM\tPOS\tP\tN\n')
                for association in associations[trait]:
                    f.write('{}\t{}\t{}\t{}\n'.format(
                        association['CHR_ID'],
                        association['CHR_POS'],
                        association['P-VALUE'],
                        association['N']
                    ))
            subprocess.check_call(f'touch output/{trait}/_SUCCESS', shell=True)
    subprocess.check_call(f'aws s3 cp output/ {s3_out}/out/pigean/inputs/sumstats/gcat_trait/ --recursive', shell=True)


def remove_data(old_ids):
    for trait in old_ids:
        subprocess.check_call(f'aws s3 rm {s3_out}/out/pigean/inputs/sumstats/gcat_trait/{trait}/ --recursive', shell=True)


def run():
    download_gwas_catalog()
    traits = get_traits()
    associations = get_trait_associations(traits)

    download_rs_map()
    rs_set = {association['SNPS'] for trait_association in associations.values() for association in trait_association if 'rs' in association['SNPS']}
    rs_map = get_rs_map(rs_set)
    mapped_associations = map_trait_associations(associations, rs_map)

    min_p_associations = get_min_p_associations(mapped_associations)

    filtered_associations = filter_associations(min_p_associations)
    filtered_traits = {k: v for k, v in traits.items() if k in filtered_associations}

    download_all_sumstats()
    old_associations = get_old_associations()
    new_or_altered_ids, old_ids = get_new_or_altered_ids(filtered_associations, old_associations)

    save_data(filtered_traits, new_or_altered_ids, filtered_associations)
    remove_data(old_ids)
    shutil.rmtree('gwas_data')
    shutil.rmtree('sumstats')
    shutil.rmtree('output')


if __name__ == '__main__':
    run()
