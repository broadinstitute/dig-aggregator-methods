#!/usr/bin/python3
import argparse
import anndata as ad
import gzip
import os
from scipy.sparse import csc_matrix, vstack
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_data(dataset):
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/single_cell/{dataset}/norm_counts.tsv.gz', 'inputs/'])
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/single_cell/{dataset}/sample_metadata.tsv.gz', 'inputs/'])


def get_metadata_maps():
    cell_type_map = {}
    donor_map = {}
    with gzip.open('inputs/sample_metadata.tsv.gz', 'rt') as f:
        header = f.readline().strip().split('\t')
        for line in f:
            json_line = dict(zip(header, line.strip().split('\t')))
            cell_type_map[json_line['ID']] = json_line['cell_type__kp']
            donor_map[json_line['ID']] = json_line['donor_id']
    return cell_type_map, donor_map


def get_sparse_array(cell_type_map, donor_map):
    with gzip.open('inputs/norm_counts.tsv.gz', 'rt') as f:
        cells = f.readline().strip().split('\t')[1:]
        gene, data = f.readline().strip().split('\t', 1)
        genes = [gene]
        A = csc_matrix([list(map(float, data.split('\t')))])
        count = 1
        idx = 0
        B = []
        for line in f:
            if count == 100:  # Just sufficiently small to reduce max memory load
                A = vstack([A, csc_matrix(B)])
                B = []
                count = 0
                print(idx)
                idx += 1
            gene, data = line.strip().split('\t', 1)
            if gene not in genes:
                line_to_append = list(map(float, data.split('\t')))
                B.append(line_to_append)
                count += 1
                genes.append(gene)
        A = vstack([A, csc_matrix(B)])
    return ad.AnnData(
        A.T,
        obs={
            'obs_names': cells,
            'cell_type__kp': [cell_type_map[cell] for cell in cells],
            'donor_id': [donor_map[cell] for cell in cells]
        },
        var={
            'var_names': genes
        })


def upload(dataset, adata):
    adata.write_h5ad('data.h5ad')
    subprocess.check_call(['aws', 's3', 'cp', 'data.h5ad', f'{s3_out}/out/single_cell/staging/h5ad/{dataset}/'])
    os.remove('data.h5ad')
    shutil.rmtree('inputs')


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default=None, required=True, type=str,
                        help="Dataset name")
    args = parser.parse_args()

    download_data(args.dataset)
    cell_type_map, donor_map = get_metadata_maps()
    adata = get_sparse_array(cell_type_map, donor_map)
    upload(args.dataset, adata)


if __name__ == '__main__':
    run()
