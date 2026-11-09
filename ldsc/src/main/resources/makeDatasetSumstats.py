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
frq_files = f'{downloaded_files}/frq'

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

valid_chromosomes = {str(i) for i in range(1, 23)}


def get_s3_dirs(phenotype, ancestry, dataset):
    return [f'{s3_in}/out/metaanalysis/variants/{phenotype}/dataset={dataset}/ancestry={ancestry}/']


def get_single_csv(s3_dir):
    subprocess.check_call(['aws', 's3', 'cp', s3_dir, f'{phenotype_files}/', '--recursive', '--exclude=_SUCCESS', '--exclude=metadata'])
    with open(f'{phenotype_files}/input.csv', 'w') as f_out:
        header = True
        for file in glob.glob(f'{phenotype_files}/*/part-*', recursive=True):
            filename, file_extension = os.path.splitext(file)
            if file_extension == '.zst':
                subprocess.check_call(['zstd', '--rm', '-d', file])
                file = filename  # zst gone from resulting file to use
            with open(file, 'r') as f_in:
                if not header:
                    _ = f_in.readline()
                header = False
                shutil.copyfileobj(f_in, f_out)
            os.remove(file)


def upload(phenotype, ancestry, dataset):
    s3_dir = f'{s3_out}/out/ldsc/dataset_sumstats/{phenotype}/{ancestry}/{dataset}'
    subprocess.check_call(['aws', 's3', 'cp', f'{phenotype_files}/output.log', f'{s3_dir}/ldsc.log'])
    subprocess.check_call(['aws', 's3', 'cp', f'{phenotype_files}/output.sumstats.gz', f'{s3_dir}/ldsc.sumstats.gz'])


def remove_files():
    for file in ['input.csv', 'input.txt', 'output.log', 'output.sumstats.gz']:
        if os.path.exists(f'{phenotype_files}/{file}'):
            os.remove(f'{phenotype_files}/{file}')


def get_snp_map():
    snp_map = {}
    with open(f'{downloaded_files}/snp.csv', 'r') as f:
        header = f.readline()
        for full_row in f.readlines():
            row = full_row.split('\t')
            snp_map[row[1].strip()] = row[0].strip()
    return snp_map


def get_frq_map(ancestry):
    frq_map = {}
    for chromosome in range(1, 23):
        with open(f'{downloaded_files}/frq/{ancestry_map[ancestry]}/chr.{chromosome}.frq', 'r') as f:
            header = f.readline().strip().split('\t')
            for full_row in f.readlines():
                json_row = dict(zip(header, full_row.strip().split('\t')))
                frq_map[json_row['SNP']] = float(json_row['MAF'])
    return frq_map


def stream_to_txt(snp_map, frq_map):
    line_count = 0
    with open(f'{phenotype_files}/input.csv', 'r') as f_in:
        header = f_in.readline().strip().split('\t')
        with open(f'{phenotype_files}/input.txt', 'w') as f_out:
            line_template = '{}\t{}\t{}\t{}\t{}\t{}\t{}\n'
            f_out.write(line_template.format('MarkerName', 'Allele1', 'Allele2', 'p', 'beta', 'N', 'FRQ'))
            for raw_line in f_in:
                line = dict(zip(header, raw_line.strip().split('\t')))
                if 'varId' in line and \
                        line['varId'] in snp_map \
                        and snp_map[line['varId']] in frq_map \
                        and line['beta'] is not None and \
                        line['chromosome'] in valid_chromosomes:
                    line_string = line_template.format(
                        snp_map[line['varId']],
                        line['reference'].lower(),
                        line['alt'].lower(),
                        line['pValue'],
                        line['beta'],
                        line['n'],
                        frq_map[snp_map[line['varId']]]
                    )
                    f_out.write(line_string)
                    line_count += 1
    return line_count


def create_sumstats():
    subprocess.check_call([
        'python3', f'{ldsc_files}/munge_sumstats.py',
        '--sumstats', f'{phenotype_files}/input.txt',
        '--keep-maf',
        '--out', f'{phenotype_files}/output',
        '--merge-alleles', f'{snp_file}/w_hm3.snplist'
    ])


def run(phenotype, ancestry, dataset, snp_map, frq_map):
    for s3_dir in get_s3_dirs(phenotype, ancestry, dataset):
        print(f'Using directory: {s3_dir}')
        get_single_csv(s3_dir)
        total_lines = stream_to_txt(snp_map, frq_map)
        if total_lines > 0:
            create_sumstats()
            upload(phenotype, ancestry, dataset)
            return True
        remove_files()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    parser.add_argument('--ancestry', default=None, required=True, type=str,
                        help="Ancestry, should be two letter version (e.g. EU) and will be made upper.")
    parser.add_argument('--dataset', default=None, required=True, type=str)
    args = parser.parse_args()
    phenotype = args.phenotype
    ancestry = args.ancestry
    dataset = args.dataset
    if ancestry not in ancestry_map:
        raise Exception(f'Invalid ancestry ({ancestry}), must be one of {", ".join(ancestry_map.keys())}')

    snp_map = get_snp_map()
    frq_map = get_frq_map(ancestry)
    print(f'Created SNP map ({len(snp_map)} variants)')

    run(phenotype, ancestry, dataset, snp_map, frq_map)
    remove_files()


if __name__ == '__main__':
    main()
