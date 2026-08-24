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
    cell_types = list(set(cell_type_map.values()))
    with gzip.open('inputs/norm_counts.tsv.gz', 'rt') as f:
        cells = f.readline().strip().split('\t')[1:]
        cells_idx_dict = {cell_type: [idx for idx, cell in enumerate(cells) if cell_type_map[cell] == cell_type] for cell_type in cell_types}
        gene, data = f.readline().strip().split('\t', 1)
        genes = [gene]
        formatted_data = list(map(float, data.split('\t')))
        A = csc_matrix([formatted_data])
        A_dict = {cell_type: csc_matrix([[formatted_data[idx] for idx in cells_idx_dict[cell_type]]]) for cell_type in cell_types}
        count = 1
        idx = 0
        B = []
        B_dict = {cell_type: [] for cell_type in cell_types}
        for line in f:
            if count == 100:  # Just sufficiently small to reduce max memory load
                A = vstack([A, csc_matrix(B)])
                for cell_type in cell_types:
                    A_dict[cell_type] = vstack([A_dict[cell_type], csc_matrix(B_dict[cell_type])])
                B = []
                B_dict = {cell_type: [] for cell_type in cell_types}
                count = 0
                idx += 1
            gene, data = line.strip().split('\t', 1)
            if gene not in genes:
                line_to_append = list(map(float, data.split('\t')))
                B.append(line_to_append)
                for cell_type in cell_types:
                    B_dict[cell_type].append([line_to_append[idx] for idx in cells_idx_dict[cell_type]])
                count += 1
                genes.append(gene)
        A = vstack([A, csc_matrix(B)])
        for cell_type in cell_types:
            A_dict[cell_type] = vstack([A_dict[cell_type], csc_matrix(B_dict[cell_type])])
    return ad.AnnData(
        A.T,
        obs={
            'obs_names': cells,
            'cell_type__kp': [cell_type_map[cell] for cell in cells],
            'donor_id': [donor_map[cell] for cell in cells]
        },
        var={
            'var_names': genes
        }), {
        cell_type: ad.AnnData(
            A_dict[cell_type].T,
            obs={
                'obs_names': [cells[idx] for idx in cells_idx_dict[cell_type]],
                'cell_type__kp': [cell_type_map[cells[idx]] for idx in cells_idx_dict[cell_type]],
                'donor_id': [donor_map[cells[idx]] for idx in cells_idx_dict[cell_type]]
            },
            var={
                'var_names': genes
            }) for cell_type in cell_types
    }


def upload(dataset, adata, cell_type_adata_map):
    adata.write_h5ad('data.h5ad')
    subprocess.check_call(['aws', 's3', 'cp', 'data.h5ad', f'{s3_out}/out/single_cell/staging/h5ad/{dataset}/'])
    os.remove('data.h5ad')
    for cell_type, adata in cell_type_adata_map.items():
        adata.write_h5ad(f'{cell_type}.h5ad')
        subprocess.check_call(['aws', 's3', 'cp', f'{cell_type}.h5ad', f'{s3_out}/out/single_cell/staging/h5ad/{dataset}/'])
        os.remove(f'{cell_type}.h5ad')
    shutil.rmtree('inputs')


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default=None, required=True, type=str,
                        help="Dataset name")
    args = parser.parse_args()

    download_data(args.dataset)
    cell_type_map, donor_map = get_metadata_maps()
    adata, cell_type_adata_dict = get_sparse_array(cell_type_map, donor_map)
    upload(args.dataset, adata, cell_type_adata_dict)


if __name__ == '__main__':
    run()
