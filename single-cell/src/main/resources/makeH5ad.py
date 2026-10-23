#!/usr/bin/python3
import argparse
import anndata as ad
import gzip
import json
import os
from scipy.sparse import csc_matrix, vstack
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_data(dataset, cell_type):
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/out/single_cell/staging/split/{dataset}/{cell_type}/raw_counts.tsv.gz', 'inputs/'])
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/out/single_cell/staging/split/{dataset}/{cell_type}/raw_counts.metadata.tsv.gz', 'inputs/'])
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/single_cell/{dataset}/column_map.json', 'inputs/'])


def get_column_map():
    with open('inputs/column_map.json', 'r') as f:
        return json.load(f)


def get_metadata_maps():
    col_map = get_column_map()
    study_map = {}
    with gzip.open('inputs/raw_counts.metadata.tsv.gz', 'rt') as f:
        header = f.readline().strip().split('\t')
        for line in f:
            json_line = dict(zip(header, line.strip().split('\t')))
            study_map[json_line[col_map['cell_id']]] = json_line[col_map['study_id']]
    return study_map


def get_sparse_array(cell_type, study_map):
    with gzip.open('inputs/raw_counts.tsv.gz', 'rt') as f:
        cells = f.readline().strip().split('\t')[1:]
        genes = []
        A_dict = []
        for line in f:
            gene, data = line.strip().split('\t', 1)
            if gene not in genes:
                line_to_append = list(map(int, data.split('\t')))
                A_dict.append(line_to_append)
                genes.append(gene)
    return ad.AnnData(
        csc_matrix(A_dict).T,
        obs={
            'obs_names': cells,
            'cell_type': [cell_type for _ in cells],
            'study': [study_map[cell] for cell in cells]
        },
        var={
            'var_names': genes
        }
    )


def upload(dataset, cell_type, adata):
    adata.write_h5ad('data.h5ad')
    subprocess.check_call(['aws', 's3', 'cp', 'data.h5ad', f'{s3_out}/out/single_cell/staging/h5ad/{dataset}/{cell_type}/'])
    os.remove('data.h5ad')
    shutil.rmtree('inputs')


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default=None, required=True, type=str,
                        help="Dataset name")
    parser.add_argument('--cell-type', default=None, required=True, type=str,
                        help="Cell Type")
    args = parser.parse_args()

    download_data(args.dataset, args.cell_type)
    study_map = get_metadata_maps()
    adata = get_sparse_array(args.cell_type, study_map)
    upload(args.dataset, args.cell_type, adata)


if __name__ == '__main__':
    run()
