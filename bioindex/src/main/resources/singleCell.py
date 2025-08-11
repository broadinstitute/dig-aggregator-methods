#!/usr/bin/python3
import argparse
import gzip
import json
import math
from multiprocessing import Pool
import numpy as np
import os
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']

cpus = 8

metadata_cell_key = 'ID'
coordinate_labels = ['X', 'Y']
non_float_gene_fields = ['datasetId', 'gene', 'cell_type__kp']

number_maps = {
    'int': int,
    'float': lambda x: round(float(x), 3)
}


def fetch_dataset_metadata():
    with open('raw/dataset_metadata.json', 'r') as f:
        return json.load(f)


def fetch_metadata():
    with gzip.open('raw/sample_metadata.tsv.gz', 'rt') as f:
        header = f.readline().strip().split('\t')
        possible_label_dict = {label: idx for idx, label in enumerate(header)}
        index_lists = {label: [] for label in possible_label_dict}
        set_lists = {label: [] for label in possible_label_dict}
        index_dict = {label: dict() for label in possible_label_dict}
        for line in f:
            split_line = [a.strip() for a in line.split('\t')]  # can have empty cells at the end of the line
            for label in possible_label_dict:
                label_value = split_line[possible_label_dict[label]]
                if label_value not in index_dict[label]:
                    index_dict[label][label_value] = len(set_lists[label])
                    set_lists[label].append(label_value)
                index_lists[label].append(index_dict[label][label_value])
        return index_lists, set_lists, index_dict


def fetch_coordinates(index_lists, set_lists, index_dict):
    return {label: [float(set_lists[label][idx]) for idx in index_lists[label]] for label in coordinate_labels}


def get_is_float(example):
    try:
        _ = float(example)
        return True
    except:
        return False


def filter_metadata(index_lists, set_lists, index_dict, max_categories):
    for label in list(index_lists.keys()):
        is_float = get_is_float(next(iter(set_lists[label])))
        if (is_float and len(set_lists[label]) > max_categories) or \
                (len(set_lists[label]) <= 1 and ''.join(set_lists[label]) == ''):
            print(f'Popping {label}')
            index_lists.pop(label)
            set_lists.pop(label)
            index_dict.pop(label)
    return index_lists, set_lists, index_dict


def output_metadata(set_lists, index_lists):
    fields = {
        metadata_cell_key: set_lists[metadata_cell_key],
        'metadata_labels': {label: data for label, data in set_lists.items() if label != metadata_cell_key},
        'metadata': {label: data for label, data in index_lists.items() if label != metadata_cell_key},
    }
    with gzip.open('processed/fields.json.gz', 'wt') as f:
        json.dump(fields, f)


def output_coordinates(index_lists, coordinates):
    with gzip.open('processed/coordinates.tsv.gz', 'wt') as f:
        f.write('\t'.join(coordinate_labels) + '\n')
        for idx in range(len(index_lists[metadata_cell_key])):
            coords_line = '\t'.join([str(round(coordinates[label][idx], 3)) for label in coordinate_labels])
            f.write(f'{coords_line}\n')


def format_p_values(p_value, p_value_adj):
    if p_value == 0.0:
        p_value = np.nextafter(0, 1)
    if p_value_adj == 0.0:
        p_value_adj = np.nextafter(0, 1)
    return p_value, p_value_adj


def format_marker_genes_data(json_data):
    for key, value in json_data.items():
        if key not in non_float_gene_fields:
            json_data[key] = float(value)
    cell_type = json_data['cell_type__kp']
    json_data['cell_type'] = cell_type
    json_data.pop('cell_type__kp')
    p_value, p_value_adj = format_p_values(json_data['p_value'], json_data['p_value_adj'])
    json_data['p_value'] = p_value
    json_data['p_value_adj'] = p_value_adj
    return json_data


def fetch_marker_genes():
    marker_genes = []
    with open('raw/marker_genes.tsv', 'r') as f_in:
        header = f_in.readline().strip().split('\t')
        for line in f_in:
            marker_genes.append(format_marker_genes_data(dict(zip(header, line.strip().split('\t')))))
    return marker_genes


def fetch_top_marker_genes():
    top_marker_genes = []
    with open('raw/marker_genes.dotplot_calculated.tsv', 'r') as f_in:
        header = f_in.readline().strip().split('\t')
        for line in f_in:
            top_marker_genes.append(format_marker_genes_data(dict(zip(header, line.strip().split('\t')))))
    return top_marker_genes


def filter_marker_genes(marker_genes):
    return [marker_gene for marker_gene in marker_genes if marker_gene['p_value_adj'] < 0.01]


def save_marker_genes(marker_genes, filename):
    with gzip.open(f'processed/{filename}.json.gz', 'wt') as f:
        for marker_gene in marker_genes:
            f.write(f'{json.dumps(marker_gene)}\n')


def file_iter(dataset, infile, outfile, cell_indexes, number_map):
    with gzip.open(infile, 'rt') as f_in:
        header = f_in.readline().strip().split('\t')[1:]
        genex_indexes = {cell_indexes[column_cell]: matrix_idx for matrix_idx, column_cell in enumerate(header)}
        part_num = 0
        part_count = 0
        f_out = open(f'raw/count_files/part-{str(part_num).zfill(5)}', 'w')
        for line in f_in:
            f_out.write(line)
            part_count += 1
            if part_count == 300:
                part_in = f'raw/count_files/part-{str(part_num).zfill(5)}'
                part_out = outfile.format(str(part_num).zfill(5))
                yield part_in, part_out, dataset, genex_indexes, number_map
                part_count = 0
                part_num += 1
                f_out = open(f'raw/count_files/part-{str(part_num).zfill(5)}', 'w')
        part_in = f'raw/count_files/part-{str(part_num).zfill(5)}'
        part_out = outfile.format(str(part_num).zfill(5))
        yield part_in, part_out, dataset, genex_indexes, number_map


def process_file(args):
    infile, outfile, dataset, genex_indexes, number_map = args
    print(infile, outfile)
    with open(infile, 'rt') as f_in:
        with open(outfile, 'w') as f_out:
            for line in f_in:
                split_line = line.strip().split('\t')
                expression = list(map(number_maps[number_map], split_line[1:]))
                sorted_expression = [expression[genex_indexes[i]] for i in range(len(genex_indexes))]
                expression_str = ','.join(map(str, sorted_expression))
                gene = split_line[0]
                f_out.write(f'{{"dataset": "{dataset}", '
                            f'"gene": "{gene}", '
                            f'"expression": [{expression_str}]}}\n')
    os.remove(infile)


def fetch_and_output_expression(dataset, cell_indexes, infile, outfile, number_map):
    os.mkdir('raw/count_files')
    with Pool(cpus) as p:
        list(p.imap(process_file, file_iter(dataset, infile, outfile, cell_indexes, number_map)))
    shutil.rmtree('raw/count_files')


def upload(dataset):
    subprocess.check_call(['aws', 's3', 'cp', 'processed/fields.json.gz', f'{s3_bioindex}/raw/single_cell/{dataset}/'])
    subprocess.check_call(['aws', 's3', 'cp', 'processed/coordinates.tsv.gz', f'{s3_bioindex}/raw/single_cell/{dataset}/'])
    subprocess.check_call(['aws', 's3', 'cp', 'processed/marker_genes.json.gz', f'{s3_bioindex}/raw/single_cell/{dataset}/'])
    subprocess.check_call(['aws', 's3', 'cp', 'processed/marker_genes_full.json.gz', f'{s3_bioindex}/raw/single_cell/{dataset}/'])
    subprocess.check_call(['aws', 's3', 'rm', f'{s3_bioindex}/single_cell/gene_norm/{dataset}/', '--recursive'])
    subprocess.check_call(['aws', 's3', 'cp', 'processed/gene_norm/', f'{s3_bioindex}/single_cell/gene_lognorm/{dataset}/', '--recursive'])
    shutil.rmtree('raw')
    shutil.rmtree('processed')


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--dataset', type=str, required=True)
    args = opts.parse_args()

    f_in = f'{s3_in}/single_cell/{args.dataset}/'
    subprocess.check_call(['aws', 's3', 'cp', f_in, 'raw', '--recursive'])

    metadata = fetch_dataset_metadata()
    max_categories = max(metadata['totalBiosamples'], metadata['totalDonors'])

    os.makedirs('processed', exist_ok=True)
    index_lists, set_lists, index_dict = fetch_metadata()
    coordinates = fetch_coordinates(index_lists, set_lists, index_dict)
    index_lists, set_lists, index_dict = filter_metadata(index_lists, set_lists, index_dict, max_categories)

    output_metadata(set_lists, index_lists)
    output_coordinates(index_lists, coordinates)

    marker_genes = fetch_marker_genes()
    top_marker_genes = fetch_top_marker_genes()
    filtered_marker_genes = filter_marker_genes(marker_genes)
    save_marker_genes(filtered_marker_genes, 'marker_genes_full')
    save_marker_genes(top_marker_genes, 'marker_genes')

    cells = index_dict[metadata_cell_key]
    os.mkdir('processed/gene_norm')
    gene_norm_out = f'processed/gene_norm/part-{{}}.json'
    fetch_and_output_expression(args.dataset, cells, 'raw/norm_counts.tsv.gz', gene_norm_out, 'float')

    upload(args.dataset)


if __name__ == '__main__':
    main()
