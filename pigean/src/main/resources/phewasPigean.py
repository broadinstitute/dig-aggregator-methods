#!/usr/bin/python3
import argparse
import os
import subprocess

downloaded_files = '/mnt/var/pigean'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_data(phenotype, sigma, gene_set_size):
    file_path = f'{s3_in}/out/pigean/staging/factor/{phenotype}/sigma={sigma}/size={gene_set_size}'
    subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/gc.out', '.'])


def download_gs(sigma, gene_set_size):
    file_path = f'{s3_in}/out/pigean/staging/combined_gs'
    gs_file = f'gs_{sigma}_{gene_set_size}.tsv'
    subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/{gs_file}', '.'])
    return gs_file


def get_factor_cols():
    with open('gc.out', 'r') as f:
        headers = f.readline().strip().split('\t')
        return headers[6:]


def run_phewas(gs_file):
    cmd = [
        'python3', f'{downloaded_files}/factor_phewas.py',
        '--factors-in', 'gc.out',
        '--factors-gene-id-col', 'Gene',
        '--factors-gene-factor-cols', ','.join(get_factor_cols()),
        '--filter-to-factor-genes',
        '--gene-stats-in', gs_file,
        '--gene-stats-id-col', 'gene',
        '--gene-stats-pheno-col', 'trait',
        '--gene-stats-assoc-stat-col', 'huge',
        '--output-file', 'phewas.out',
        '--output-provenance-file', 'phewas.provenance.out',
        '--log-file', 'phewas.log'
    ]
    subprocess.check_call(cmd)


def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def upload_data(phenotype, sigma, gene_set_size):
    file_path = f'{s3_out}/out/pigean/staging/phewas/{phenotype}/sigma={sigma}/size={gene_set_size}/'
    for file in ['phewas.out', 'phewas.provenance.out', 'phewas.log']:
        subprocess.check_call(['aws', 's3', 'cp', file, file_path])
        os.remove(file)
    success(file_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    parser.add_argument('--sigma', default=None, required=True, type=str,
                        help="Sigma power (0, 2, 4).")
    parser.add_argument('--gene-set-size', default=None, required=True, type=str,
                        help="gene-set-size (small, medium, or large).")
    args = parser.parse_args()

    download_data(args.phenotype, args.sigma, args.gene_set_size)
    gs_file = download_gs(args.sigma, args.gene_set_size)
    try:
        run_phewas(gs_file)
        upload_data(args.phenotype, args.sigma, args.gene_set_size)
    except Exception as e:
        print(e)
        print('Error')
    os.remove('gc.out')
    os.remove(gs_file)


if __name__ == '__main__':
    main()




