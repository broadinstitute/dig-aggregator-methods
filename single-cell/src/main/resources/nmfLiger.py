#!/usr/bin/python3
import anndata as ad
import argparse
import os
import shutil
import subprocess

downloaded_files = '/mnt/var/single_cell'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

MIN_CELLS = 10

dataset_to_tissue = {
    'islet_of_Langerhans_scRNA_v3-5': 'islet'
}



def download_data(dataset, cell_type):
    path = f'{s3_in}/out/single_cell/staging/h5ad/{dataset}/{cell_type}/data.h5ad'
    subprocess.check_call(['aws', 's3', 'cp', path, 'inputs/data.h5ad'])


def get_num_cells():
    adata = ad.read_h5ad('inputs/data.h5ad')
    return adata.n_obs


def run_nmf_liger(tissue, cell_type):
    try:
        subprocess.check_call([
            'Rscript', f'{downloaded_files}/run_liger_nmf_h5ad.R',
            '--h5ad', 'inputs/data.h5ad',
            '--signatures', f'{downloaded_files}/{tissue}_liger.gmt',
            '--outdir', 'outputs',
            '--auto_k',
            '--blacklist_gmt', f'{downloaded_files}/cmdkp_all_tissues_minimal_bad_cell_qc_signatures.with_celltypes.gmt',
            '--batch_col', 'study',
            '--celltype_label', f'{cell_type}',
            '--umi_col', 'QC:nCount_RNA',
            '--mito_col', 'QC:percent.mt',
            '--min_umi', '500',
            '--max_umi','50000',
            '--max_mito', '10',
            '--min_cells_per_gene', f'{MIN_CELLS}',
            '--min_genes_per_cell', '200',
            '--scale_factor', '10000'
        ])
    except Exception as e:
        print(e)
        print('ERROR in Liger subprocess')


def upload_data(tissue, cell_type, dataset):
    path = f'{s3_out}/out/single_cell/staging/nmf/liger/{tissue}/{cell_type}/{dataset}'
    subprocess.check_call(['aws', 's3', 'cp', 'outputs/', path, '--recursive'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset')
    parser.add_argument('--cell-type')
    args = parser.parse_args()

    tissue = dataset_to_tissue[args.dataset]

    download_data(args.dataset, args.cell_type)
    num_cells = get_num_cells()
    if num_cells > MIN_CELLS:
        run_nmf_liger(tissue, args.cell_type)
        if os.path.exists('outputs/factor_report.txt'):
            upload_data(tissue, args.cell_type, args.dataset)
    shutil.rmtree('inputs')
    if os.path.exists('outputs'):
        shutil.rmtree('outputs')


if __name__ == '__main__':
    main()
