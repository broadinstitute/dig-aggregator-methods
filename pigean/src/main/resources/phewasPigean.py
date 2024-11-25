#!/usr/bin/python3
import argparse
import glob
import os
import re
import shutil
import subprocess

downloaded_files = '/mnt/var/pigean'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_data(trait_group, phenotype, sigma, gene_set_size):
    file_path = f'{s3_in}/out/pigean/staging/factor/{trait_group}/{phenotype}/sigma={sigma}/size={gene_set_size}'
    subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/gc.out', '.'])


def get_factor_cols():
    with open('gc.out', 'r') as f:
        headers = f.readline().strip().split('\t')
        factors_with_genes = set()
        for line in f:
            factors_with_genes |= {line.strip().split('\t')[5]}
        return [header for header in headers[7:] if header in factors_with_genes]


def run_phewas(gs_files):
    os.makedirs('out', exist_ok=True)
    for gs_file in gs_files:
        number = re.findall('.*_([0-9]*).tsv')[0]
        cmd = [
            'python3', f'{downloaded_files}/factor_phewas.py',
            '--factors-in', 'gc.out',
            '--factors-gene-id-col', 'Gene',
            '--factors-gene-factor-cols', ','.join(get_factor_cols()),
            '--filter-to-factor-genes',
            '--gene-stats-in', f'{downloaded_files}/{gs_file}',
            '--gene-stats-id-col', 'gene',
            '--gene-stats-pheno-col', 'trait',
            '--gene-stats-assoc-stat-col', 'huge',
            '--output-file', f'out/phewas_{number}.out',
            '--output-provenance-file', f'out/phewas_{number}.provenance.out',
            '--log-file', f'out/phewas_{number}.log'
        ]
        subprocess.check_call(cmd)


def combine_phewas():
    with open('out/phewas.out', 'w') as f_out:
        for phewas in glob.glob('out/phewas_*.out'):
            with open(phewas, 'r') as f_in:
                shutil.copyfileobj(f_in, f_out)
            os.remove(phewas)


def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def upload_data(trait_group, phenotype, sigma, gene_set_size):
    file_path = f'{s3_out}/out/pigean/staging/phewas/{trait_group}/{phenotype}/sigma={sigma}/size={gene_set_size}/'
    subprocess.check_call(f'aws s3 cp out/ {file_path} --recursive', shell=True)
    shutil.rmtree('out')
    success(file_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--trait-group', default=None, required=True, type=str,
                        help="Input phenotype group.")
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    parser.add_argument('--sigma', default=None, required=True, type=str,
                        help="Sigma power (0, 2, 4).")
    parser.add_argument('--gene-set-size', default=None, required=True, type=str,
                        help="gene-set-size (small, medium, or large).")
    args = parser.parse_args()

    download_data(args.trait_group, args.phenotype, args.sigma, args.gene_set_size)
    gs_files = glob.glob(f'gs_{args.sigma}_{args.gene_set_size}_*.tsv')
    try:
        run_phewas(gs_files)
        upload_data(args.trait_group, args.phenotype, args.sigma, args.gene_set_size)
    except Exception as e:
        print(e)
        print('Error')
    os.remove('gc.out')


if __name__ == '__main__':
    main()




