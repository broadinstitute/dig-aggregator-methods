#!/usr/bin/python3
import argparse
from boto3 import session
import glob
import json
import os
import shutil
import sqlalchemy
import subprocess


# This map is used to map portal ancestries to g1000 ancestries
ancestry_map = {
    'AA': 'AFR',
    'AF': 'AFR',
    'SSAF': 'AFR',
    'HS': 'AMR',
    'EA': 'EAS',
    'EU': 'EUR',
    'SA': 'SAS',
    'GME': 'SAS',
    'Mixed': 'EUR'
}

downloaded_files = '/mnt/var/ldsc'
ldsc_files = f'{downloaded_files}/ldsc'
phenotype_files = '.'
snp_file = f'{downloaded_files}/snps'


def get_s3_dir(dataset, phenotype):
    return f's3://dig-giant-sandbox/variants/GWAS/{dataset}/{phenotype}/'


def get_single_json_file(s3_dir, dataset, phenotype):
    subprocess.check_call(['aws', 's3', 'cp', s3_dir, f'{phenotype_files}/', '--recursive', '--exclude=_SUCCESS', '--exclude=metadata'])
    with open(f'{phenotype_files}/{dataset}_{phenotype}.json', 'w') as f_out:
        for file in glob.glob(f'{phenotype_files}/part-*', recursive=True):
            filename, file_extension = os.path.splitext(file)
            if file_extension == '.zst':
                subprocess.check_call(['zstd', '-d', file])
                file = filename  # zst gone from resulting file to use
            with open(file, 'r') as f_in:
                shutil.copyfileobj(f_in, f_out)
            os.remove(file)


def get_ancestry_sex(dataset, phenotype):
    with open(f'{phenotype_files}/{dataset}_{phenotype}.json', 'r') as f:
        first_line_json = json.loads(f.readline())
        return first_line_json['ancestry'], first_line_json['sex']


def upload_and_remove_files(dataset, phenotype, ancestry, sex):
    s3_dir = f's3://dig-giant-sandbox/out/ldsc/sumstats/{phenotype}/ancestry={ancestry}/sex={sex}/'
    subprocess.check_call(['aws', 's3', 'cp', f'{phenotype_files}/{phenotype}_{ancestry}_{sex}.log', s3_dir])
    subprocess.check_call(['aws', 's3', 'cp', f'{phenotype_files}/{phenotype}_{ancestry}_{sex}.sumstats.gz', s3_dir])
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', s3_dir])
    os.remove('_SUCCESS')
    for file in glob.glob(f'{phenotype_files}/{phenotype}_{ancestry}_{sex}.*'):
        os.remove(file)
    for file in glob.glob(f'{phenotype_files}/{dataset}_{phenotype}.*'):
        os.remove(file)


def get_snp_map():
    snp_map = {}
    with open(f'{downloaded_files}/snp.csv', 'r') as f:
        header = f.readline()
        for full_row in f.readlines():
            row = full_row.split('\t')
            snp_map[row[1].strip()] = row[0].strip()
    return snp_map


def stream_to_txt(dataset, phenotype, snp_map):
    line_count = 0
    with open(f'{phenotype_files}/{dataset}_{phenotype}.json', 'r') as f_in:
        with open(f'{phenotype_files}/{dataset}_{phenotype}.txt', 'w') as f_out:
            line_template = '{}\t{}\t{}\t{}\t{}\t{}\n'
            f_out.write(line_template.format('MarkerName', 'Allele1', 'Allele2', 'p', 'beta', 'N'))
            json_string = f_in.readline()
            while len(json_string) > 0:
                for json_substring in json_string.replace('}{', '}\n{').splitlines():
                    line = json.loads(json_substring)
                    if 'varId' in line and line['varId'] in snp_map and line['beta'] is not None:
                        line_string = line_template.format(
                            snp_map[line['varId']],
                            line['reference'].lower(),
                            line['alt'].lower(),
                            line['pValue'],
                            line['beta'],
                            line['n']
                        )
                        f_out.write(line_string)
                        line_count += 1
                json_string = f_in.readline()
    return line_count


def create_sumstats(dataset, phenotype, ancestry, sex):
    subprocess.check_call([
        'python3', f'{ldsc_files}/munge_sumstats.py',
        '--sumstats', f'{phenotype_files}/{dataset}_{phenotype}.txt',
        '--out', f'{phenotype_files}/{phenotype}_{ancestry}_{sex}',
        '--merge-alleles', f'{snp_file}/w_hm3.snplist'
    ])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default=None, required=True, type=str,
                        help="Dataset name")
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")

    args = parser.parse_args()
    dataset = args.dataset
    phenotype = args.phenotype
    s3_dir = get_s3_dir(dataset, phenotype)
    print(f'Using directory: {s3_dir}')

    get_single_json_file(s3_dir, dataset, phenotype)
    ancestry, sex = get_ancestry_sex(dataset, phenotype)

    if ancestry not in ancestry_map:
        raise Exception(f'Invalid ancestry ({ancestry}), must be one of {", ".join(ancestry_map.keys())}')

    snp_map = get_snp_map()
    print(f'Created SNP map ({len(snp_map)} variants)')
    total_lines = stream_to_txt(dataset, phenotype, snp_map)
    if total_lines > 0:
        create_sumstats(dataset, phenotype, ancestry, sex)
        upload_and_remove_files(dataset, phenotype, ancestry, sex)


if __name__ == '__main__':
    main()
