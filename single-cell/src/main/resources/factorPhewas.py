#!/usr/bin/python3
import argparse
import os
import shutil
import subprocess

downloaded_files = '/mnt/var/pigean'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download(dataset, cell_type):
    path_in = f'{s3_out}/out/single_cell/staging/factor_matrix/{dataset}/{cell_type}'
    subprocess.check_call(['aws', 's3', 'cp', f'{path_in}', f'input/{cell_type}/', '--recursive'])


def run_phewas(cell_type):
    os.makedirs(f'output/{cell_type}', exist_ok=True)
    subprocess.check_call(['python3.11', '-m', 'eaggl', 'factor', '--run-factor-phewas',
                           '--factor-phewas-anchor-covariate', 'none',
                           '--factor-gene-clusters-in', os.path.abspath(f'input/{cell_type}/factor_matrix_gene_loadings.tsv'),
                           '--gene-phewas-stats-in', f'{downloaded_files}/gs_mouse_msigdb.tsv',
                           '--gene-phewas-stats-id-col', 'gene',
                           '--gene-phewas-stats-pheno-col', 'trait',
                           '--gene-phewas-stats-combined-col', 'combined',
                           '--factor-phewas-stats-out', os.path.abspath(f'./output/{cell_type}/phewas_gene_loadings.txt')],
                          cwd=f'{downloaded_files}/pigean/src')


def upload(dataset, cell_type):
    path_out = f'{s3_out}/out/single_cell/staging/factor_phewas/{dataset}/{cell_type}'
    subprocess.check_call(['aws', 's3', 'cp', f'output/{cell_type}/', f'{path_out}', '--recursive'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default=None, required=True, type=str,
                        help="Dataset name")
    parser.add_argument('--cell-type', default=None, required=True, type=str,
                        help="Cell Type")
    args = parser.parse_args()
    download(args.dataset, args.cell_type)

    run_phewas(args.cell_type)

    upload(args.dataset, args.cell_type)
    shutil.rmtree('input')
    shutil.rmtree('output')


if __name__ == '__main__':
    main()
