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
    'mouse_msigdb_phi5': 'mouse_msigdb'
}


def download(dataset, cell_type):
    path_in = f'{s3_in}/out/single_cell/staging/downsample/{dataset}/{cell_type}/'
    subprocess.check_call(['aws', 's3', 'cp', f'{path_in}', f'input/{cell_type}/', '--recursive'])


def run_factor_matrix(cell_type, model):
    os.makedirs(f'output/{cell_type}/{model}', exist_ok=True)
    try:
        subprocess.check_call(['python', f'{downloaded_files}/factor_matrix.py',
                               '--matrix_file', f'input/{cell_type}/norm_counts.sample.tsv.gz',
                               '--output_prefix', f'output/{cell_type}/{model}/factor_matrix',
                               '--row_name', 'gene',
                               '--col_name', 'cell',
                               '--n_hvgs', '3000',
                               '--phi', f'{model_to_phi[model]}'])
    except Exception as e:
        print('Failed to run Factor Matrix')
        print(e)


def upload(dataset, cell_type, model):
    path_out = f'{s3_out}/out/single_cell/staging/factor_matrix/{dataset}/{cell_type}/{model}'
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
    download(args.dataset, args.cell_type)

    run_factor_matrix(args.cell_type, args.model)

    upload(args.dataset, args.cell_type, args.model)
    shutil.rmtree('input')
    shutil.rmtree('output')


if __name__ == '__main__':
    main()
