#!/usr/bin/python3
import argparse
import gzip
import numpy as np
import os
import re
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download(dataset):
    subprocess.check_call(f'aws s3 cp "{s3_in}/single_cell/{dataset}/sample_metadata.tsv.gz" input/', shell=True)
    subprocess.check_call(f'aws s3 cp "{s3_in}/single_cell/{dataset}/norm_counts.tsv.gz" input/', shell=True)


def format_cell_type(cell_type):
    return re.sub(r'[^a-zA-Z0-9_-]', '', cell_type.replace(' ', '_').lower())


def get_p_inv():
    p_inv = {}
    with gzip.open('input/sample_metadata.tsv.gz', 'rt') as f_in:
        header = f_in.readline().strip().split('\t')
        idx = header.index('cell_type__kp')
        for line in f_in:
            cell_type = line.strip().split('\t', idx + 1)[-2]
            cell_type_str = format_cell_type(cell_type)
            if cell_type_str not in p_inv:
                p_inv[cell_type_str] = 0
            p_inv[cell_type_str] += 1 / 3000
    return p_inv


def get_cells():
    p_inv = get_p_inv()
    cell_type_cells = {}
    with gzip.open('input/sample_metadata.tsv.gz', 'rt') as f_in:
        header = f_in.readline().strip().split('\t')
        idx = header.index('cell_type__kp')
        for line in f_in:
            cell_type = line.strip().split('\t', idx + 1)[-2]
            cell_type_str = format_cell_type(cell_type)
            if np.random.rand() < 1 / p_inv[cell_type_str]:
                if cell_type_str not in cell_type_cells:
                    cell_type_cells[cell_type_str] = set()
                cell_type_cells[cell_type_str] |= {line.strip().split('\t')[0]}
    return cell_type_cells


def write_metadata(cell_type_cells):
    f_outs = {cell_type: gzip.open(f'output/{cell_type}/sample_metadata.sample.tsv.gz', 'wt') for cell_type in cell_type_cells}
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
    f_outs = {cell_type: gzip.open(f'output/{cell_type}/norm_counts.sample.tsv.gz', 'wt') for cell_type in cell_type_cells}
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


def upload(dataset):
    subprocess.check_call(f'aws s3 rm "{s3_out}/out/single_cell/staging/downsample/{dataset}/" --recursive', shell=True)
    subprocess.check_call(f'aws s3 cp output/ "{s3_out}/out/single_cell/staging/downsample/{dataset}/" --recursive', shell=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default=None, required=True, type=str,
                        help="Dataset name")
    args = parser.parse_args()
    download(args.dataset)

    cell_type_cells = get_cells()
    for cell_type in cell_type_cells:
        os.makedirs(f'output/{cell_type}', exist_ok=True)

    write_metadata(cell_type_cells)
    write_lognorm_counts(cell_type_cells)

    upload(args.dataset)
    shutil.rmtree('input')
    shutil.rmtree('output')


if __name__ == '__main__':
    main()
