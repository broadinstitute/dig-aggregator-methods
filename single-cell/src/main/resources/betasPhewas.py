#!/usr/bin/python3
import argparse
import glob
import os
import shutil
import subprocess

downloaded_files = '/mnt/var/single_cell'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

def download_data():
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/out/single_cell/gene_sets/', 'gene_sets/', '--recursive'])


def run_pigean(dataset, kind, tissue, cell_type):
    os.makedirs(f'combined/{kind}', exist_ok=True)
    if kind == 'cell_state':
        path = f'gene_sets/{kind}/{tissue}/{cell_type}/*/gene_sets.gmt'
    else:
        path = f'gene_sets/{kind}/{tissue}/{cell_type}/{dataset}/gene_sets.gmt'
    if len(glob.glob(path)) > 0:
        with open(f'combined/{kind}/{cell_type}.gmt', 'w') as f:
            header = None
            for file in glob.glob(path):
                with open(file, 'r') as f_in:
                    if header is None:
                        header = f_in.readline()
                        f.write(header)
                    else:
                        _ = f_in.readline()
                    for line in f_in:
                        f.write(line)
        cmd = [
            'python3.11', f'{downloaded_files}/dig-cell-state-scoring/scripts/run_api_pigean.py',
            '--gmt-dir', f'combined/{kind}',
            '--out-dir', f'outputs/{kind}',
            '--combined-out', f'outputs/{kind}/combined_pigean.tsv.gz',
            '--kind', 'curated' if kind == 'cell_state' else 'program',
            '--tissue', tissue,
            '--dataset', dataset,
            '--model', 'mouse_msigdb',
            '--python', 'python3.11',
            '--pythonpath', f'{downloaded_files}/pigean/pigean/src',
            '--multi-y-in', f'{downloaded_files}/pigean/gs_mouse_msigdb.tsv',
            '--multi-y-id-col', 'gene',
            '--multi-y-pheno-col', 'trait',
            '--multi-y-log-bf-col', 'log_bf',
            '--multi-y-combined-col', 'combined',
            '--multi-y-prior-col', 'huge',
            '--trait-blacklist-in', 'auto',
            '--gene-universe-in', f'{downloaded_files}/pigean/NCBI37.3.plink.gene.loc',
        ]
        subprocess.check_call(cmd)


def upload_data(tissue, cell_type, dataset):
    subprocess.check_call(['aws', 's3', 'cp', 'outputs/', f'{s3_in}/out/single_cell/staging/betas_phewas/{tissue}/{cell_type}/{dataset}/', '--recursive'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tissue')
    parser.add_argument('--cell-type')
    parser.add_argument('--dataset')
    args = parser.parse_args()

    download_data()
    run_pigean(args.dataset, 'cell_state', args.tissue, args.cell_type)
    run_pigean(args.dataset, 'programs', args.tissue, args.cell_type)
    upload_data(args.tissue, args.cell_type, args.dataset)
    shutil.rmtree('outputs')
    shutil.rmtree('gene_sets')


if __name__ == '__main__':
    main()
