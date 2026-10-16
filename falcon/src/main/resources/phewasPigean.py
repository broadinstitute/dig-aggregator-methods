#!/usr/bin/python3
import argparse
import glob
import gzip
import os
import re
import shutil
import subprocess

downloaded_files = '/mnt/var/pigean'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_data(trait_group, phenotype, model):
    file_path = f'{s3_in}/out/falcon/staging/factor/{trait_group}/{phenotype}/{model}'
    subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/gene_clusters.out.gz', '.'])


def get_factor_cols():
    with gzip.open('gene_clusters.out.gz', 'rt') as f:
        return [a for a in f.readline().strip().split('\t') if re.match(r'Factor[0-9]*', a) is not None]


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
    file_path = f'{s3_out}/out/pigean/staging/phewas/{trait_group}/{phenotype}/{gene_set_size}/'
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
    parser.add_argument('--gene-set-size', default=None, required=True, type=str,
                        help="gene-set-size (small, medium, or large).")
    args = parser.parse_args()

    download_data(args.trait_group, args.phenotype, args.gene_set_size)
    gs_files = glob.glob(f'{downloaded_files}/gs_{args.gene_set_size}_*.tsv')
    try:
        run_phewas(args.trait_group, gs_files)
        combine_phewas()
        upload_data(args.trait_group, args.phenotype, args.gene_set_size)
    except Exception as e:
        print(e)
        print('Error')
    os.remove('gc.out')


if __name__ == '__main__':
    main()




