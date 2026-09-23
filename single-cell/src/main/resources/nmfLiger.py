#!/usr/bin/python3
import argparse
import os
import shutil
import subprocess

downloaded_files = '/mnt/var/single_cell'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

dataset_to_tissue = {
    'FNIH_Artery_scRNA_v3': 'artery',
    'FNIH_BoneMarrow_scRNA_v1': 'bone_marrow',
    'FNIH_Bone_scRNA_v1.0': 'bone',
    'FNIH_Heart_scRNA_v4.0': 'heart',
    'FNIH_Hypothalamus_scRNA_v2.2': 'hypothalamus',
    'FNIH_Kidney_scRNA_v2.2': 'kidney',
    'FNIH_Liver_scRNA_v3.2': 'liver',
    'FNIH_Liver_scRNA_v4.0': 'liver',
    'FNIH_Muscle_scRNA_v2.2': 'muscle',
    'FNIH_PLN_scRNA_v1.0': 'pln',
    'FNIH_Pancreas_scRNA_v3': 'pancreas',
    'FNIH_SAT_scRNA_v2.2': 'sat',
    'FNIH_TendonLigament_scRNA_v2': 'tendon',
    'FNIH_VAT_scRNA_v2.2': 'vat',
}


def download_data(dataset, cell_type):
    path = f'{s3_in}/out/single_cell/staging/h5ad/{dataset}/{cell_type}/data.h5ad'
    subprocess.check_call(['aws', 's3', 'cp', path, 'inputs/data.h5ad'])


def run_nmf_liger(tissue, cell_type):
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
        '--min_cells_per_gene', '10',
        '--min_genes_per_cell', '200',
        '--scale_factor', '10000'
    ])


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
    run_nmf_liger(tissue, args.cell_type)
    upload_data(tissue, args.cell_type, args.dataset)
    shutil.rmtree('inputs')


if __name__ == '__main__':
    main()
