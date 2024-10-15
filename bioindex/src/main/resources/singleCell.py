#!/usr/bin/python3
import argparse
import gzip
import json
from multiprocessing import Pool
import os
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']
gene_path = '/mnt/var/single_cell/genes.json'

cpus = 8

metadata_cell_key = 'NAME'
coordinate_cell_key = 'NAME'
coordinate_labels = ['X', 'Y']

number_maps = {
    'int': int,
    'float': lambda x: round(float(x), 3)
}


def get_gene_map():
    out = {}
    with open(gene_path, 'r') as f:
        for line in f:
            json_line = json.loads(line.strip())
            out[json_line['name']] = json_line['symbol']
    return out


def fetch_metadata():
    with open('raw/metadata.tsv', 'r') as f:
        header = f.readline().strip().split('\t')
        possible_label_dict = {label: idx for idx, label in enumerate(header)}
        index_lists = {label: [] for label in possible_label_dict}
        set_lists = {label: [] for label in possible_label_dict}
        index_dict = {label: dict() for label in possible_label_dict}
        for line in f:
            split_line = [a.strip() for a in line.split('\t')]  # can have empty cells at the end of the line
            split_line = [split_line[0]] + ['_'.join(split_line[1:3])] + split_line[3:]
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


def fetch_coordinates(cell_indexes):
    with open('raw/coordinates.tsv', 'r') as f:
        header = f.readline().strip().split('\t')
        label_to_header_idx = {label: header.index(label) for label in coordinate_labels}
        cell_idx = header.index(coordinate_cell_key)
        coordinate_data = {label: dict() for label in coordinate_labels}
        for line in f:
            split_line = line.strip().split('\t')
            line_cell_idx = cell_indexes[split_line[cell_idx]]
            for label in coordinate_labels:
                coordinate_data[label][line_cell_idx] = split_line[label_to_header_idx[label]]
        return {label: [float(coordinate_data[label][idx]) for idx in range(len(cell_indexes))]
                for label in coordinate_labels}


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


def file_iter(dataset, infile, outfile, cell_indexes, number_map, gene_map):
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
                yield part_in, part_out, dataset, genex_indexes, number_map, gene_map
                part_count = 0
                part_num += 1
                f_out = open(f'raw/count_files/part-{str(part_num).zfill(5)}', 'w')
        yield part_in, part_out, dataset, genex_indexes, number_map, gene_map


def process_file(args):
    infile, outfile, dataset, genex_indexes, number_map, gene_map = args
    print(infile, outfile)
    with open(infile, 'rt') as f_in:
        with open(outfile, 'w') as f_out:
            for line in f_in:
                split_line = line.strip().split('\t')
                expression = list(map(number_maps[number_map], split_line[1:]))
                sorted_expression = [expression[genex_indexes[i]] for i in range(len(genex_indexes))]
                expression_str = ','.join(map(str, sorted_expression))
                ensembl = split_line[0].split('.')[0]
                gene = gene_map.get(ensembl, ensembl)
                f_out.write(f'{{"dataset": "{dataset}", '
                            f'"gene": "{gene}", '
                            f'"expression": [{expression_str}]}}\n')
    os.remove(infile)


def fetch_and_output_expression(dataset, cell_indexes, infile, outfile, number_map, gene_map):
    os.mkdir('raw/count_files')
    with Pool(cpus) as p:
        list(p.imap(process_file, file_iter(dataset, infile, outfile, cell_indexes, number_map, gene_map)))
    shutil.rmtree('raw/count_files')


def upload(dataset):
    subprocess.check_call(['aws', 's3', 'cp', 'processed/fields.json.gz', f'{s3_bioindex}/raw/single_cell/{dataset}/'])
    subprocess.check_call(['aws', 's3', 'cp', 'processed/coordinates.tsv.gz', f'{s3_bioindex}/raw/single_cell/{dataset}/'])
    subprocess.check_call(['aws', 's3', 'rm', f'{s3_bioindex}/single_cell/gene_lognorm/{dataset}/', '--recursive'])
    subprocess.check_call(['aws', 's3', 'cp', 'processed/gene_lognorm/', f'{s3_bioindex}/single_cell/gene_lognorm/{dataset}/', '--recursive'])
    shutil.rmtree('raw')
    shutil.rmtree('processed')


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--dataset', type=str, required=True)
    args = opts.parse_args()

    f_in = f'{s3_in}/single_cell/{args.dataset}/'
    subprocess.check_call(['aws', 's3', 'cp', f_in, 'raw', '--recursive'])

    os.mkdir('processed')
    index_lists, set_lists, index_dict = fetch_metadata()
    index_lists, set_lists, index_dict = filter_metadata(index_lists, set_lists, index_dict)

    coordinates = fetch_coordinates(index_dict[metadata_cell_key])
    output_metadata(set_lists, index_lists)
    output_coordinates(index_lists, coordinates)

    cells = index_dict[metadata_cell_key]
    os.mkdir('processed/gene_lognorm')
    gene_lognorm_out = f'processed/gene_lognorm/part-{{}}.json'
    gene_map = get_gene_map()
    fetch_and_output_expression(args.dataset, cells, 'raw/lognorm_counts.tsv.gz', gene_lognorm_out, 'float', gene_map)

    upload(args.dataset)


if __name__ == '__main__':
    main()
