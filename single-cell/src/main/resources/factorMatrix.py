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
    subprocess.check_call(f'aws s3 cp {path_in} inputs/{cell_type}/ --recursive', shell=True)


def factor_and_phewas(cell_type, model):
    os.makedirs(f'outputs/{cell_type}/{model}', exist_ok=True)
    subprocess.check_call(f'python {downloaded_files}/factor_matrix.py '
                          f'--matrix_file inputs/{cell_type}/norm_counts.sample.tsv.gz '
                          f'--output_prefix outputs/{cell_type}/{model}/factor_matrix '
                          '--n_hvgs 3000 '
                          f'--phi {model_to_phi[model]}', shell=True)

    with open(f'outputs/{cell_type}/{model}/factor_matrix_row_loadings.tsv', 'r') as f:
        factor_str = ','.join(f.readline().strip().split('\t')[1:])

    subprocess.check_call(f'python {downloaded_files}/factor_phewas.py '
                          f'--factors-in outputs/{cell_type}/{model}/factor_matrix_row_loadings.tsv '
                          '--factors-gene-id-col row_name '
                          f'--factors-gene-factor-cols {factor_str} '
                          '--pheno-batch-size 2400 '
                          f'--gene-stats-in {downloaded_files}/gs_{model_to_gene_stats[model]}.tsv '
                          '--gene-stats-id-col gene '
                          '--gene-stats-pheno-col trait '
                          '--gene-stats-assoc-stat-col combined '
                          f'--output-file outputs/{cell_type}/{model}/phewas_row_loadings.txt', shell=True)


def upload(dataset, cell_type, model):
    path_out = f'{s3_out}/out/single_cell/staging/factor_matrix/{dataset}/{cell_type}/{model}'
    subprocess.check_call(f'aws s3 cp output/{cell_type}/{model}/ "{path_out}" --recursive', shell=True)


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

    factor_and_phewas(args.cell_type, args.model)

    upload(args.dataset, args.cell_type, args.model)
    shutil.rmtree('input')
    shutil.rmtree('output')


if __name__ == '__main__':
    main()
