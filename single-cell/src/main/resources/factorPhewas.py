#!/usr/bin/python3
import argparse
import os
import shutil
import subprocess

downloaded_files = '/mnt/var/pigean'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

model_to_phi = {
    'mouse_msigdb_phi1': 1,
    'mouse_msigdb_phi5': 5
}

model_to_gene_stats = {
    'mouse_msigdb_phi1': 'mouse_msigdb',
    'mouse_msigdb_phi5': 'mouse_msigdb',
    'mouse_msigdb': 'mouse_msigdb'
}


def download(dataset, cell_type, model):
    path_in = f'{s3_out}/out/single_cell/staging/factor_matrix/{dataset}/{cell_type}/{model}'
    subprocess.check_call(['aws', 's3', 'cp', f'{path_in}', f'input/{cell_type}/{model}/', '--recursive'])


def run_phewas(cell_type, model):
    os.makedirs(f'output/{cell_type}/{model}', exist_ok=True)
    subprocess.check_call(['python3.11', '-m', 'eaggl', 'factor', '--run-factor-phewas',
                           '--factor-phewas-anchor-covariate', 'none',
                           '--factor-gene-clusters-in', os.path.abspath(f'input/{cell_type}/{model}/factor_matrix_gene_loadings.tsv'),
                           '--gene-phewas-stats-in', f'{downloaded_files}/gs_{model_to_gene_stats[model]}.tsv',
                           '--gene-phewas-stats-id-col', 'gene',
                           '--gene-phewas-stats-pheno-col', 'trait',
                           '--gene-phewas-stats-combined-col', 'combined',
                           '--factor-phewas-stats-out', os.path.abspath(f'./output/{cell_type}/{model}/phewas_gene_loadings.txt')],
                          cwd=f'{downloaded_files}/pigean/src')


def upload(dataset, cell_type, model):
    path_out = f'{s3_out}/out/single_cell/staging/factor_phewas/{dataset}/{cell_type}/{model}'
    subprocess.check_call(['aws', 's3', 'cp', f'output/{cell_type}/{model}/', f'{path_out}', '--recursive'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default=None, required=True, type=str,
                        help="Dataset name")
    parser.add_argument('--cell-type', default=None, required=True, type=str,
                        help="Cell Type")
    parser.add_argument('--model', default=None, required=True, type=str,
                        help="Model")
    args = parser.parse_args()
    download(args.dataset, args.cell_type, args.model)

    run_phewas(args.cell_type, args.model)

    upload(args.dataset, args.cell_type, args.model)
    shutil.rmtree('input')
    shutil.rmtree('output')


if __name__ == '__main__':
    main()
