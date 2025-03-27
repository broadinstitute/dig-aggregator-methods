#!/usr/bin/python3
import glob
from multiprocessing import Pool
import os
import re
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']
cpus = 8


def download_all_data():
    cmd = ['aws', 's3', 'cp', f'{s3_in}/out/pigean/staging/pigean/', './data/', '--recursive',
           '--exclude="*"', '--include="*/*/*/gs.out"']
    subprocess.check_call(' '.join(cmd), shell=True)


def convert_line(phenotype, headers, line):
    line_dict = dict(zip(headers, line.strip().split('\t')))
    line_dict['phenotype'] = phenotype
    if 'positive_control' in line_dict:
        line_dict['huge_score_gwas'] = line_dict['positive_control']
    keys = ['phenotype', 'Gene', 'combined', 'huge_score_gwas', 'log_bf']
    values = [line_dict[key] for key in keys]
    if 'NA' not in values:
        return '\t'.join(values) + '\n'


def convert(file):
    phenotype = re.findall('data/[^/]*/([^/]*)/.*', file)[0]
    with open(file, 'r') as f_in:
        headers = f_in.readline().strip().split('\t')
        with open(re.sub(r'\.out$', '.tsv', file), 'w') as f_out:
            for line in f_in:
                converted_line = convert_line(phenotype, headers, line)
                if converted_line is not None:
                    f_out.write(converted_line)


def convert_all_data():
    with Pool(cpus) as p:
        p.map(convert, glob.glob('data/*/*/*/gs.out'))


def combine(gene_set_size):
    if not os.path.exists('out'):
        os.mkdir('out')
    all_files = glob.glob(f'data/*/*/{gene_set_size}/gs.tsv')
    with open(f'out/gs_{gene_set_size}_huge.tsv', 'w') as f_out:
        f_out.write('trait\tgene\tcombined\thuge\tlog_bf\n')
        for file in all_files:
            with open(file, 'r') as f_in:
                for line in f_in:
                    split_line = line.strip().split('\t')
                    if float(split_line[3]) > 1.0:
                        f_out.write(line)


def combine_all():
    sizes = set()
    for file in glob.glob('data/*/*/*/gs.tsv'):
        sizes |= {re.findall('data/.*/.*/([^/]*)/gs.tsv', file)[0]}
    for gene_set_size in sizes:
        combine(gene_set_size)
    shutil.rmtree('data')


def upload_data():
    subprocess.check_call(['aws', 's3', 'cp', 'out/', f'{s3_out}/out/pigean/staging/combined_gs/', '--recursive'])
    shutil.rmtree('out')


def main():
    download_all_data()
    convert_all_data()
    combine_all()
    upload_data()


if __name__ == '__main__':
    main()
