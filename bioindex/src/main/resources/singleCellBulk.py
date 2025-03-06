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

metadata_cell_key = 'sample_id'

number_maps = {
    'int': int,
    'float': lambda x: round(float(x), 3)
}


def fetch_metadata(cells):
    with open('raw/metadata.tsv', 'r') as f:
        header = f.readline().strip().split('\t')
        possible_label_dict = {label: idx for idx, label in enumerate(header)}
        index_lists = {label: [] for label in possible_label_dict}
        set_lists = {label: [] for label in possible_label_dict}
        index_dict = {label: dict() for label in possible_label_dict}
        for line in f:
            split_line = [a.strip() for a in line.split('\t')]  # can have empty cells at the end of the line
            if len(cells) == 0 or split_line[possible_label_dict[metadata_cell_key]] in cells:
                for label in possible_label_dict:
                    label_value = split_line[possible_label_dict[label]]
                    if label_value not in index_dict[label]:
                        index_dict[label][label_value] = len(set_lists[label])
                        set_lists[label].append(label_value)
                    index_lists[label].append(index_dict[label][label_value])
        return index_lists, set_lists, index_dict


def filter_metadata(index_lists, set_lists, index_dict):
    max_numb = 100
    for label in list(index_lists.keys()):
        if (len(set_lists[label]) >= max_numb and label != metadata_cell_key) or \
                (len(set_lists[label]) <= 1 and ''.join(set_lists[label]) == ''):
            index_lists.pop(label)
            set_lists.pop(label)
            index_dict.pop(label)
    return index_lists, set_lists, index_dict


def convert_dea():
    with gzip.open('processed/dea.tsv.gz', 'wt') as f_out:
        f_out.write('datasetId\tcomparison\tcomparison_id\tgene\tlogFoldChange\t-log10P\n')
        with gzip.open('raw/dea.tsv.gz', 'rt') as f_in:
            header = f_in.readline().strip().split('\t')
            for line in f_in:
                json_line = dict(zip(header, line.strip().split('\t')))
                f_out.write('{}\t{}\t{}\t{}\t{}\t{}\n'.format(
                    json_line['datasetId'],
                    json_line['dea_comp_name'],
                    json_line['dea_comp_id'],
                    json_line['Gene'],
                    float(json_line['log_fold_change']),
                    float(json_line['-log10(p_value_adj)'])
                ))


def output_metadata(set_lists, index_lists):
    fields = {
        metadata_cell_key: set_lists[metadata_cell_key],
        'metadata_labels': {label: data for label, data in set_lists.items() if label != metadata_cell_key},
        'metadata': {label: data for label, data in index_lists.items() if label != metadata_cell_key},
    }
    with gzip.open('processed/fields.json.gz', 'wt') as f:
        json.dump(fields, f)


def get_diff_exp(infile, dataset, cell_indexes, number_map):
    lognorm_data = []
    with gzip.open(infile, 'rt') as f_in:
        cell_header = f_in.readline().strip().split('\t')[13:]
        genex_indexes = {cell_indexes[column_cell]: matrix_idx for matrix_idx, column_cell in enumerate(cell_header)}
        for line in f_in:
            split_line = line.strip().split('\t')
            expression = list(map(number_maps[number_map], split_line[13:]))
            if len(expression) == len(genex_indexes):
                sorted_expression = [expression[genex_indexes[i]] for i in range(len(genex_indexes))]
                if len(split_line[2].strip()) > 0:
                    json_line = {
                        'dataset': dataset,
                        'gene': split_line[4],
                        'comparison_field': split_line[1],
                        'comparison': split_line[2],
                        'comparison_id': split_line[3],
                        'log10FDR': float(split_line[12]),
                        'logFoldChange': float(split_line[5]),
                        'expression': sorted_expression
                    }
                    lognorm_data.append(json_line)
    return lognorm_data


def sort_and_save_diff_exp_data(diff_exp_data, file_out):
    with open(file_out, 'w') as f:
        for json_line in sorted(diff_exp_data, key=lambda x: (x['comparison'], -x['log10FDR'], -x['logFoldChange'])):
            f.write(f'{json.dumps(json_line)}\n')


def get_melted_data(infile, file_out, dataset):
    melted_data = {}
    with gzip.open(infile, 'rt') as f_in:
        header = f_in.readline().strip().split('\t')
        for line in f_in:
            split_line = line.strip().split('\t')
            if split_line[1] not in melted_data:
                melted_data[split_line[1]] = []
            data_dict = dict(zip(header[2:], split_line[2:]))
            data_dict['sample_id'] = split_line[0]
            melted_data[split_line[1]].append(data_dict)
    with open(file_out, 'w') as f_out:
        for gene, melted_counts in melted_data.items():
            for melted_count in melted_counts:
                melted_count['dataset'] = dataset
                melted_count['gene'] = gene
                f_out.write(f'{json.dumps(melted_count)}\n')


def upload(dataset):
    raw_path = f'{s3_bioindex}/raw/single_cell_bulk/{dataset}/'
    for file in ['fields.json.gz', 'dea.tsv.gz']:
        if os.path.exists(f'processed/{file}'):
            subprocess.check_call(['aws', 's3', 'cp', f'processed/{file}', raw_path])
    for bioindex in ['diff_exp', 'melted']:
        bioindex_path = f'{s3_bioindex}/single_cell/bulk/{bioindex}/{dataset}'
        if os.path.exists(f'processed/{bioindex}/part-00000.json'):
            subprocess.check_call(['aws', 's3', 'cp', f'processed/{bioindex}/part-00000.json', f'{bioindex_path}/'])
    shutil.rmtree('raw')
    shutil.rmtree('processed')


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--dataset', type=str, required=True)
    args = opts.parse_args()

    f_in = f'{s3_in}/bulk_rna/{args.dataset}/'
    subprocess.check_call(['aws', 's3', 'cp', f_in, 'raw', '--recursive'])

    cells = set()
    if os.path.exists('raw/dea_for_heatmap.tsv.gz'):
        with gzip.open('raw/dea_for_heatmap.tsv.gz', 'rt') as f_in:
            cells = set(f_in.readline().strip().split('\t')[13:])

    os.makedirs('processed', exist_ok=True)
    index_lists, set_lists, index_dict = fetch_metadata(cells)
    index_lists, set_lists, index_dict = filter_metadata(index_lists, set_lists, index_dict)
    output_metadata(set_lists, index_lists)

    if os.path.exists('raw/dea.tsv.gz'):
        convert_dea()

    if os.path.exists('raw/dea_for_heatmap.tsv.gz'):
        cells = index_dict[metadata_cell_key]
        os.makedirs('processed/diff_exp', exist_ok=True)
        gene_diff_exp = f'processed/diff_exp/part-00000.json'
        diff_exp_data = get_diff_exp('raw/dea_for_heatmap.tsv.gz', args.dataset, cells, 'float')
        sort_and_save_diff_exp_data(diff_exp_data, gene_diff_exp)

    if os.path.exists('raw/norm_counts.melted.tsv.gz'):
        os.makedirs('processed/melted', exist_ok=True)
        gene_melted_out = f'processed/melted/part-00000.json'
        get_melted_data('raw/norm_counts.melted.tsv.gz', gene_melted_out, args.dataset)

    upload(args.dataset)


if __name__ == '__main__':
    main()
