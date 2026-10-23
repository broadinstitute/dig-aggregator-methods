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


def download_all_data(model):
    cmd = ['aws', 's3', 'cp', f'{s3_in}/out/pigean/staging/pigean/', './data/', '--recursive',
           '--exclude="*"', f'--include="*/*/{model}/gs.out"']
    subprocess.check_call(' '.join(cmd), shell=True)


def convert_line(phenotype, headers, line):
    line_dict = dict(zip(headers, line.strip().split('\t')))
    line_dict['phenotype'] = phenotype
    if 'positive_control' in line_dict:
        line_dict['huge_score_gwas'] = line_dict['positive_control']
    if 'huge_score_exomes' in line_dict:
        line_dict['huge_score_gwas'] = line_dict['huge_score_exomes']
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
    os.remove(file)


def convert_all_data(model):
    with Pool(cpus) as p:
        p.map(convert, glob.glob(f'data/*/*/{model}/gs.out'))


def combine(model):
    os.makedirs('out', exist_ok=True)
    all_files = glob.glob(f'data/*/*/{model}/gs.tsv')
    with open(f'out/gs_{model}.tsv', 'w') as f_out:
        f_out.write('trait\tgene\tcombined\thuge\tlog_bf\n')
        for file in all_files:
            with open(file, 'r') as f_in:
                for line in f_in:
                    split_line = line.strip().split('\t')
                    if float(split_line[2]) > 1.0:
                        f_out.write(line)


def upload_data(model):
    subprocess.check_call(['aws', 's3', 'cp', f'out/gs_{model}.tsv', f'{s3_out}/out/pigean/staging/combined/'])
    shutil.rmtree('out')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', type=str, required=True)
    args = parser.parse_args()

    download_all_data(args.model)
    convert_all_data(args.model)
    combine(args.model)
    upload_data(args.model)


if __name__ == '__main__':
    main()
