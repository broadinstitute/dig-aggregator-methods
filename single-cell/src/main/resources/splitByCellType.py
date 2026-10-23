#!/usr/bin/python3
import argparse
import gzip
import json

import math
import os
import re
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download(dataset):
    path_in = f'{s3_in}/single_cell/{dataset}'
    subprocess.check_call(['aws', 's3', 'cp', f'{path_in}/sample_metadata.tsv.gz', 'input/'])
    subprocess.check_call(['aws', 's3', 'cp', f'{path_in}/column_map.json', 'input/'])
    subprocess.check_call(['aws', 's3', 'cp', f'{path_in}/norm_counts.tsv.gz', 'input/'])


def format_cell_type(cell_type):
    return re.sub(r'[^a-zA-Z0-9_-]', '', cell_type.replace(' ', '_').lower())


def get_column_map():
    with open('input/column_map.json', 'r') as f:
        return json.load(f)


def get_cells():
    col_map = get_column_map()
    cell_type_cells = {}
    ncount_map = {}
    raw_study = {}
    study_map = {}
    with gzip.open('input/sample_metadata.tsv.gz', 'rt') as f_in:
        header = f_in.readline().strip().split('\t')
        for line in f_in:
            json_line = dict(zip(header, line.strip().split('\t')))
            cell_type = json_line[col_map['cell_type']]
            cell_id = json_line[col_map['cell_id']]
            cell_type_str = format_cell_type(cell_type)
            if cell_type_str not in cell_type_cells:
                cell_type_cells[cell_type_str] = set()
            cell_type_cells[cell_type_str] |= {cell_id}
            study = json_line[col_map['study_id']]
            study_map[cell_id] = study
            if study not in raw_study:
                raw_study[study] = True
            n_count = json_line[col_map['ncount']]
            if len(n_count) > 0 and float(n_count) % 1 == 0:
                ncount_map[cell_id] = float(json_line[col_map['ncount']])
            else:
                raw_study[study] = False
    return cell_type_cells, ncount_map, raw_study, study_map


def write_norm_metadata(cell_type_cells):
    f_outs = {cell_type: gzip.open(f'output/{cell_type}/norm_counts.metadata.tsv.gz', 'wt') for cell_type in cell_type_cells}
    with gzip.open('input/sample_metadata.tsv.gz', 'rt') as f:
        header = f.readline()
        for f_out in f_outs.values():
            f_out.write(header)
        for line in f:
            cell, _ = line.strip().split('\t', 1)
            for cell_type, cells in cell_type_cells.items():
                if cell in cells:
                    f_outs[cell_type].write(line)
    for f_out in f_outs.values():
        f_out.close()


def write_lognorm_counts(cell_type_cells):
    f_outs = {cell_type: gzip.open(f'output/{cell_type}/norm_counts.tsv.gz', 'wt') for cell_type in cell_type_cells}
    with gzip.open('input/norm_counts.tsv.gz', 'rt') as f_in:
        header = f_in.readline().strip().split('\t')
        idxs = {}
        for cell_type, f_out in f_outs.items():
            idxs[cell_type] = [0] + [idx + 1 for idx, cell in enumerate(header[1:]) if cell in cell_type_cells[cell_type]]
            stripped_line = '\t'.join([header[idx] for idx in idxs[cell_type]])
            f_out.write(f'{stripped_line}\n')
        for line in f_in:
            split_line = line.strip().split('\t')
            for cell_type, f_out in f_outs.items():
                stripped_line = '\t'.join([split_line[idx] for idx in idxs[cell_type]])
                f_out.write(f'{stripped_line}\n')
    for f_out in f_outs.values():
        f_out.close()


def write_raw_metadata(cell_type_cells, ncount_map, raw_study, study_map):
    f_outs = {cell_type: gzip.open(f'output/{cell_type}/raw_counts.metadata.tsv.gz', 'wt') for cell_type in cell_type_cells}
    with gzip.open('input/sample_metadata.tsv.gz', 'rt') as f:
        header = f.readline()
        for f_out in f_outs.values():
            f_out.write(header)
        for line in f:
            cell, _ = line.strip().split('\t', 1)
            for cell_type, cells in cell_type_cells.items():
                if cell in cells and raw_study[study_map[cell]]:
                    f_outs[cell_type].write(line)
    for f_out in f_outs.values():
        f_out.close()


def write_raw_counts(cell_type_cells, ncount_map, raw_study, study_map):
    f_outs = {cell_type: gzip.open(f'output/{cell_type}/raw_counts.tsv.gz', 'wt') for cell_type in cell_type_cells}
    with gzip.open('input/norm_counts.tsv.gz', 'rt') as f_in:
        header = f_in.readline().strip().split('\t')
        idxs = {}
        for cell_type, f_out in f_outs.items():
            idxs[cell_type] = [(idx, ncount_map[cell]) for idx, cell in enumerate(header[1:]) if cell in cell_type_cells[cell_type] and raw_study[study_map[cell]]]
            stripped_line = 'gene\t{}'.format('\t'.join([header[idx + 1] for idx, _ in idxs[cell_type]]))
            f_out.write(f'{stripped_line}\n')
        for line in f_in:
            gene, data = line.strip().split('\t', 1)
            formatted_data = list(map(float, data.split('\t')))
            for cell_type, f_out in f_outs.items():
                stripped_line = '\t'.join([str(int(round((math.exp(formatted_data[idx]) - 1) * ncount / 1E4))) for idx, ncount in idxs[cell_type]])
                f_out.write(f'{gene}\t{stripped_line}\n')
    for f_out in f_outs.values():
        f_out.close()


def upload(dataset):
    subprocess.check_call(['aws', 's3', 'rm', f'{s3_out}/out/single_cell/staging/split/{dataset}/', '--recursive'])
    subprocess.check_call(['aws', 's3', 'cp', 'output/', f'{s3_out}/out/single_cell/staging/split/{dataset}/', '--recursive'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default=None, required=True, type=str,
                        help="Dataset name")
    args = parser.parse_args()
    download(args.dataset)

    cell_type_cells, ncount_map, raw_study, study_map = get_cells()
    for cell_type in cell_type_cells:
        os.makedirs(f'output/{cell_type}', exist_ok=True)

    write_norm_metadata(cell_type_cells)
    write_lognorm_counts(cell_type_cells)
    write_raw_metadata(cell_type_cells, ncount_map, raw_study, study_map)
    write_raw_counts(cell_type_cells, ncount_map, raw_study, study_map)

    upload(args.dataset)
    shutil.rmtree('input')
    shutil.rmtree('output')


if __name__ == '__main__':
    main()
