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
    file_path = f'{s3_in}/out/pigean-old/staging/factor/{trait_group}/{phenotype}/sigma={sigma}/size={gene_set_size}'
    subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/gc.out', '.'])


idxs = {'portal': {'factor': 4, 'idx': 6}}
default_idxs = {'factor': 5, 'idx': 7}
def get_factor_cols(trait_group):
    with open('gc.out', 'r') as f:
        headers = f.readline().strip().split('\t')
        factors_with_genes = set()
        for line in f:
            factors_with_genes |= {line.strip().split('\t')[idxs.get(trait_group, default_idxs)['factor']]}
        return [header for header in headers[idxs.get(trait_group, default_idxs)['idx']:] if header in factors_with_genes]


def run_phewas(trait_group, gs_files):
    for gs_file in gs_files:
        number = re.findall('.*_([0-9]*).tsv', gs_file)[0]
        cmd = [
            'python3', f'{downloaded_files}/factor_phewas.py',
            '--factors-in', 'gc.out',
            '--factors-gene-id-col', 'Gene',
            '--factors-gene-factor-cols', ','.join(get_factor_cols(trait_group)),
            '--filter-to-factor-genes',
            '--gene-stats-in', gs_file,
            '--gene-stats-id-col', 'gene',
            '--gene-stats-pheno-col', 'trait',
            '--gene-stats-assoc-stat-col', 'huge',
            '--output-file', f'phewas_{number}.out',
            '--output-provenance-file', f'phewas.provenance.out',
            '--log-file', f'phewas.log'
        ]
        subprocess.check_call(cmd)


def combine_phewas():
    phewases = glob.glob('phewas_*.out')
    with open('phewas.out', 'w') as f_out:
        with open(phewases[0], 'r') as f:
            f_out.write(f.readline())  # header
        for phewas in phewases:
            with open(phewas, 'r') as f_in:
                _ = f_in.readline()
                shutil.copyfileobj(f_in, f_out)
            os.remove(phewas)


def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def upload_data(trait_group, phenotype, sigma, gene_set_size):
    file_path = f'{s3_out}/out/old-pigean/staging/phewas/{trait_group}/{phenotype}/sigma={sigma}/size={gene_set_size}/'
    for file in ['phewas.out', 'phewas.provenance.out', 'phewas.log']:
        subprocess.check_call(['aws', 's3', 'cp', file, file_path])
        os.remove(file)
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
    gs_files = glob.glob(f'{downloaded_files}/gs_{args.sigma}_{args.gene_set_size}_*.tsv')
    try:
        run_phewas(args.trait_group, gs_files)
        combine_phewas()
        upload_data(args.trait_group, args.phenotype, args.sigma, args.gene_set_size)
    except Exception as e:
        print(e)
        print('Error')
    os.remove('gc.out')


if __name__ == '__main__':
    main()




