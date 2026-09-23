#!/usr/bin/python3
import argparse
import gzip
import json
import os
import shutil
import subprocess

downloaded_files = '/mnt/var/single_cell'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

dataset_to_tissue = {
    'FNIH_Artery_scRNA_v3': 'artery',
    'FNIH_BoneMarrow_scRNA_v1': 'bonemarrow',
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
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/out/single_cell/staging/split/{dataset}/{cell_type}/norm_counts.tsv.gz', 'inputs/'])
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/out/single_cell/staging/split/{dataset}/{cell_type}/norm_counts.metadata.tsv.gz', 'inputs/'])
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/single_cell/{dataset}/column_map.json', 'inputs/'])


def prepare_sparse_matrix():
    cmd = [
        'python3.11', f'{downloaded_files}/dig-cell-state-scoring/scripts/convert_expression_tsv_to_sparse_10x.py',
        '--matrix-tsv', 'inputs/norm_counts.tsv.gz',
        '--out-dir', 'outputs',
        '--orientation', 'gene_by_cell',
        '--value-type', 'log1p_cp10k'
    ]
    subprocess.check_call(cmd)


def get_column_map():
    with open('inputs/column_map.json', 'r') as f:
        return json.load(f)


metadata_fields = ['cell_id', 'tissue', 'cell_type', 'dataset_id', 'donor_id', 'sample_id']
def prepare_metadata(tissue, cell_type, dataset):
    col_map = get_column_map()
    cells = []
    with gzip.open('outputs/barcodes.tsv.gz', 'rt') as f:
        for line in f:
            cells.append(line.strip())
    with gzip.open('outputs/metadata.tsv.gz', 'wt') as f_out:
        f_out.write('{}\n'.format('\t'.join(metadata_fields)))
        with gzip.open('inputs/norm_counts.metadata.tsv.gz', 'rt') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                out_line = {}
                dict_line = dict(zip(header, line.strip().split('\t')))
                if dict_line[col_map['cell_id']] in cells:
                    out_line['cell_id'] = dict_line[col_map['cell_id']]
                    out_line['tissue'] = tissue
                    out_line['cell_type'] = cell_type
                    out_line['dataset_id'] = dataset
                    out_line['donor_id'] = dict_line[col_map['donor_id']]
                    out_line['sample_id'] = dict_line[col_map['sample_id']]
                    f_out.write('{}\n'.format(
                        '\t'.join([str(out_line[k]) for k in metadata_fields])
                    ))


def upload_data(tissue, cell_type, dataset):
    subprocess.check_call(['aws', 's3', 'cp', 'outputs/', f'{s3_in}/out/single_cell/staging/mtx/{tissue}/{cell_type}/{dataset}/', '--recursive'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset')
    parser.add_argument('--cell-type')
    args = parser.parse_args()

    tissue = dataset_to_tissue[args.dataset]

    download_data(args.dataset, args.cell_type)
    prepare_sparse_matrix()
    prepare_metadata(tissue, args.cell_type, args.dataset)
    upload_data(tissue, args.cell_type, args.dataset)
    shutil.rmtree('outputs')
    shutil.rmtree('inputs')


if __name__ == '__main__':
    main()
