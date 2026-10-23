#!/usr/bin/python3
import argparse
import os
import subprocess
import shutil

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_data(dataset):
    cmd = ['aws', 's3', 'cp', f'{s3_in}/curated_cell_states/{dataset}/', 'inputs/', '--recursive']
    subprocess.check_call(cmd)


def get_tissue_cell_types(dataset):
    tissue_cell_types = []
    with open(f'inputs/cell_state_manifest.tsv', 'r') as f:
        header = f.readline().strip().split('\t')
        for line in f:
            dict_line = dict(zip(header, line.strip().split('\t')))
            tissue_cell_types.append((dict_line['tissue_id'], dict_line['cell_type_id']))
    return tissue_cell_types


def filter_cell_stats(dataset, tissue, cell_type):
    state_ids = set()
    curated_manifest_rows = []
    with open(f'inputs/cell_state_manifest.tsv', 'r') as f:
        header = f.readline().strip().split('\t')
        for line in f:
            dict_line = dict(zip(header, line.strip().split('\t')))
            if dict_line['tissue_id'] == tissue and dict_line['cell_type_id'] == cell_type:
                state_ids |= {dict_line['state_id']}
                curated_manifest_rows.append(
                    {
                        'state_name': dict_line['state_id'],
                        'tissue': tissue,
                        'cell_type': cell_type,
                        'state_class': dict_line.get('state_class', 'unknown'),
                        'is_composite_required': str(dict_line.get('is_composite_required', 'false')).lower(),
                        'signature_kind': 'curated_state',
                    }
                )

    os.makedirs(f'outputs/cell_state/{tissue}/{cell_type}/{dataset}', exist_ok=True)
    with open(f'outputs/cell_state/{tissue}/{cell_type}/{dataset}/manifest.tsv', 'w') as f:
        f.write('state_name\ttissue\tcell_type\tstate_class\tis_composite_required\tsignature_kind\n')
        for row in curated_manifest_rows:
            f.write('{}\t{}\t{}\t{}\t{}\t{}\n'.format(
                row['state_name'],
                row['tissue'],
                row['cell_type'],
                row['state_class'],
                row['is_composite_required'],
                row['signature_kind'])
            )

    curated_rows = []
    with open(f'inputs/cell_state_markers.gmt', 'r') as f:
        for line in f:
            split_line = line.strip().split('\t')
            if split_line[0] in state_ids:
                curated_rows.append((split_line[0], split_line[1], [g for g in split_line[2:] if g]))

    with open(f'outputs/cell_state/{tissue}/{cell_type}/{dataset}/gene_sets.gmt', 'w') as f:
        for row in curated_rows:
            f.write('{}\t{}\t{}\n'.format(
                row[0],
                row[1],
                '\t'.join(row[2])
            ))


def upload_data():
    cmd = ['aws', 's3', 'cp', 'outputs/', f'{s3_out}/out/single_cell/gene_sets/', '--recursive']
    subprocess.check_call(cmd)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset')
    args = parser.parse_args()

    download_data(args.dataset)
    tissue_cell_types = get_tissue_cell_types(args.dataset)
    for tissue, cell_type in tissue_cell_types:
        filter_cell_stats(args.dataset, tissue, cell_type)
    upload_data()
    shutil.rmtree('outputs')
    shutil.rmtree('inputs')


if __name__ == '__main__':
    main()
