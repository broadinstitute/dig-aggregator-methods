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
    keys = ['phenotype', 'Gene', 'combined', 'huge_score_gwas', 'log_bf']
    return '\t'.join([line_dict[key] for key in keys]) + '\n'


def convert(file):
    phenotype = re.findall('data/([^/]*)/.*', file)[0]
    with open(file, 'r') as f_in:
        headers = f_in.readline().strip().split('\t')
        with open(re.sub(r'\.out$', '.tsv', file), 'w') as f_out:
            for line in f_in:
                f_out.write(convert_line(phenotype, headers, line))


def convert_all_data():
    with Pool(cpus) as p:
        p.map(convert, glob.glob('data/*/*/*/gs.out'))


def combine(sigma, gene_set_size):
    if not os.path.exists('out'):
        os.mkdir('out')
    with open(f'out/gs_{sigma}_{gene_set_size}.tsv', 'w') as f_out:
        f_out.write('trait\tgene\tcombined\thuge\tlog_bf\n')
        for file in glob.glob(f'data/*/sigma={sigma}/size={gene_set_size}/gs.tsv'):
            with open(file, 'r') as f_in:
                shutil.copyfileobj(f_in, f_out)


def combine_all():
    sigma_sizes = set()
    for file in glob.glob('data/*/*/*/gs.tsv'):
        sigma_sizes |= {re.findall('data/.*/sigma=([^/]*)/size=([^/]*)/gs.tsv', file)[0]}
    for sigma, gene_set_size in sigma_sizes:
        combine(sigma, gene_set_size)
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
