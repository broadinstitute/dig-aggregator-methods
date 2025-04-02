#!/usr/bin/python3
import argparse
import glob
from multiprocessing import Pool
import os
import re
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']
cpus = 8


def download_all_data(gene_set_size):
    cmd = ['aws', 's3', 'cp', f'{s3_in}/out/pigean/staging/pigean/', './data/', '--recursive',
           '--exclude="*"', f'--include="*/*/{gene_set_size}/gss.out"']
    subprocess.check_call(' '.join(cmd), shell=True)


def convert_line(phenotype, headers, line):
    line_dict = dict(zip(headers, line.strip().split('\t')))
    line_dict['phenotype'] = phenotype
    keys = ['phenotype', 'Gene_Set', 'beta', 'beta_uncorrected']
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
    os.remove(file)


def convert_all_data(gene_set_size):
    with Pool(cpus) as p:
        p.map(convert, glob.glob(f'data/*/*/{gene_set_size}/gss.out'))


def combine(gene_set_size):
    if not os.path.exists('out'):
        os.mkdir('out')
    all_files = glob.glob(f'data/*/*/{gene_set_size}/gss.tsv')
    with open(f'out/gss_{gene_set_size}.tsv', 'w') as f_out:
        f_out.write('trait\tgene_set\tbeta\tbeta_uncorrected\n')
        for file in all_files:
            with open(file, 'r') as f_in:
                for line in f_in:
                    split_line = line.strip().split('\t')
                    if float(split_line[3]) > 0.01:
                        f_out.write(line)


def upload_data(gene_set_size):
    subprocess.check_call(['aws', 's3', 'cp', f'out/gss_{gene_set_size}.tsv', f'{s3_out}/out/pigean/staging/combined/'])
    shutil.rmtree('out')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--gene-set-size', type=str, required=True)
    args = parser.parse_args()

    download_all_data(args.gene_set_size)
    convert_all_data(args.gene_set_size)
    combine(args.gene_set_size)
    upload_data(args.gene_set_size)


if __name__ == '__main__':
    main()
