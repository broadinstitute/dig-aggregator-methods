#!/usr/bin/python3
import argparse
import json
import os
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download(dataset):
    path = f'{s3_in}/annotated_regions/gene_expression_levels/{dataset}'
    for file in ['metadata.tsv', 'gene_info.tsv', 'diff_expression.tsv', 'dataset_metadata', 'normalized_expression.tsv']:
        subprocess.check_call(f'aws s3 cp {path}/{file} data/{file}', shell=True)


def get_id_map():
    id_map = {}
    with open('data/metadata.tsv', 'r') as f:
        header = f.readline().strip().split('\t')
        for line in f:
            line_dict = dict(zip(header, line.strip().split('\t')))
            id_map[line_dict['ID']] = {
                'founder': line_dict['strain'],
                'sex': line_dict['sex'],
                'tissue': line_dict['tissue']
            }
    return id_map


def get_gene_map():
    gene_map = {}
    with open('data/gene_info.tsv', 'r') as f_in:
        header = f_in.readline().strip().split('\t')
        for line in f_in:
            line_dict = dict(zip(header, line.strip().split('\t')))
            gene_map[line_dict['Gene_ID']] = {
                'gene': line_dict['Gene_symbol'],
                'gene_id': line_dict['Gene_ID'],
                'chromosome': line_dict['Chr'],
                'start': int(line_dict['Start']),
                'end': int(line_dict['End']),
                'tissue': line_dict['tissue']
            }
    return gene_map


def get_p_value_map():
    p_value_map = {}
    with open('data/diff_expression.tsv', 'r') as f_in:
        header = f_in.readline().strip().split('\t')
        for line in f_in:
            line_dict = dict(zip(header, line.strip().split('\t')))
            if line_dict['Gene_ID'] not in p_value_map:
                p_value_map[line_dict['Gene_ID']] = []
            p_value_map[line_dict['Gene_ID']].append({
                'category': line_dict['category'],
                'f': line_dict['f'],
                'p_value': line_dict['P_value'],
                'p_value_adj': line_dict['P_value_adj']
            })
    return p_value_map


def process_expression(gene_map, id_map):
    os.makedirs('out', exist_ok=True)
    with open('out/diff_exp.json', 'w') as f_out:
        with open('data/normalized_expression.tsv', 'r') as f_in:
            header = f_in.readline().strip().split('\t')
            for line in f_in:
                split_line = line.strip().split('\t')
                ensembl = split_line[0]
                for id, exp_data in zip(header[1:], split_line[1:]):
                    out_dict = {
                        'gene': gene_map[ensembl]['gene'],
                        'gene_id': gene_map[ensembl]['gene_id'],
                        'chromosome': gene_map[ensembl]['chromosome'],
                        'start': gene_map[ensembl]['start'],
                        'end': gene_map[ensembl]['end'],
                        'tissue': id_map[id]['tissue'],
                        'founder': id_map[id]['founder'],
                        'sex': id_map[id]['sex'],
                        'expression': float(exp_data),
                        'sample_size': len(id_map)
                    }
                    f_out.write(f'{json.dumps(out_dict)}\n')


def process_summary_stats(gene_map, p_value_map):
    os.makedirs('out', exist_ok=True)
    with open('out/summary_stats.json', 'w') as f_out:
        for ensembl, gene_info in gene_map.items():
            out_dict = {
                'gene': gene_map[ensembl]['gene'],
                'gene_id': gene_map[ensembl]['gene_id'],
                'chromosome': gene_map[ensembl]['chromosome'],
                'start': gene_map[ensembl]['start'],
                'end': gene_map[ensembl]['end'],
                'tissue': gene_map[ensembl]['tissue'],
            }
            if ensembl in p_value_map:
                for data in p_value_map.get(ensembl, []):
                    category = data['category']
                    out_dict.update({
                        f'f_{category}': float(data['f']),
                        f'P_value_{category}': float(data['p_value']),
                        f'P_adj_{category}': float(data['p_value_adj'])
                    }
                    )
                f_out.write(f'{json.dumps(out_dict)}\n')


def copy_metadata():
    with open('data/dataset_metadata', 'r') as f_in:
        with open('out/metadata', 'w') as f_out:
            shutil.copyfileobj(f_in, f_out)


def success(path):
    subprocess.check_call('touch _SUCCESS', shell=True)
    subprocess.check_call(f'aws s3 cp _SUCCESS {path}', shell=True)
    os.remove('_SUCCESS')


def upload(dataset):
    path = f'{s3_out}/differential_expression/{dataset}/'
    subprocess.check_call(f'aws s3 cp out/ {path} --recursive', shell=True)
    success(path)
    shutil.rmtree('data')
    shutil.rmtree('out')


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--dataset', type=str, required=True)
    args = opts.parse_args()

    download(args.dataset)

    id_map = get_id_map()
    gene_map = get_gene_map()
    p_value_map = get_p_value_map()

    process_expression(gene_map, id_map)
    process_summary_stats(gene_map, p_value_map)
    copy_metadata()

    upload(args.dataset)


if __name__=='__main__':
    main()
