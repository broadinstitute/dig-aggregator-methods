#!/usr/bin/python3
import argparse
import os
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

def get_gene_set_data_map():
    subprocess.check_call('aws s3 cp s3://dig-analysis-bin/pigean/misc/gene_set_map.tsv .', shell=True)
    out = {}
    with open('gene_set_map.tsv', 'r') as f:
        for line in f:
            gene_set, gene_set_description, program = line.strip().split('\t')
            out[gene_set] = (gene_set_description, program)
    os.remove('gene_set_map.tsv')
    return out


def download_data(trait_group, phenotype, file_name, gene_set_size):
    file_path = f'{s3_in}/out/pigean/staging/pigean/{trait_group}/{phenotype}/{gene_set_size}/{file_name}'
    subprocess.check_call(['aws', 's3', 'cp', file_path, '.'])


def upload_data(trait_group, phenotype, data_type, gene_set_size):
    file_path = f'{s3_out}/out/pigean/{data_type}/{gene_set_size}/{trait_group}/{phenotype}/'
    file_out = f'{data_type}.json'
    subprocess.check_call(['aws', 's3', 'cp', file_out, file_path])
    success(file_path)
    os.remove(file_out)


def make_option(value):
    return value if value != 'NA' else 'null'


def translate_gs(json_line, trait_group, phenotype, gene_set_size):
    combined = make_option(json_line["combined"])
    huge_score = json_line["huge_score_gwas"] if 'huge_score_gwas' in json_line else json_line["positive_control"]
    if combined is not None:
        return f'{{"gene": "{json_line["Gene"]}", ' \
               f'"prior": {make_option(json_line["prior"])}, ' \
               f'"combined": {combined}, ' \
               f'"huge_score": {make_option(huge_score)}, ' \
               f'"log_bf": {make_option(json_line["log_bf"])}, ' \
               f'"n": {make_option(json_line["N"])}, ' \
               f'"trait_group": "{trait_group}", ' \
               f'"phenotype": "{phenotype}", ' \
               f'"gene_set_size": "{gene_set_size}"}}\n'


def get_translate_gss():
    gene_set_data_map = get_gene_set_data_map()
    def translate_gss(json_line, trait_group, phenotype, gene_set_size):
        beta = make_option(json_line["beta"])
        beta_uncorrected = make_option(json_line["beta_uncorrected"])
        if beta is not None and beta_uncorrected is not None and float(beta_uncorrected) != 0.0:
            description, program = gene_set_data_map[json_line["Gene_Set"]]
            return f'{{"gene_set": "{json_line["Gene_Set"]}", ' \
                   f'"gene_set_description": "{description}", ' \
                   f'"gene_set_program": "{program}", ' \
                   f'"source": "{json_line["label"]}", ' \
                   f'"beta": {beta}, ' \
                   f'"beta_uncorrected": {beta_uncorrected}, ' \
                   f'"n": {make_option(json_line["N"])}, ' \
                   f'"trait_group": "{trait_group}", ' \
                   f'"phenotype": "{phenotype}", ' \
                   f'"gene_set_size": "{gene_set_size}"}}\n'
    return translate_gss


def get_translate_ggss(trait_group, phenotype, gene_set_size):
    beta_uncorrected_map, source_map = get_ggss_maps(trait_group, phenotype, gene_set_size)
    gene_set_data_map = get_gene_set_data_map()
    def translate_ggss(json_line, trait_group, phenotype, gene_set_size):
        beta = make_option(json_line["beta"])
        combined = make_option(json_line["combined"])
        if beta is not None and combined is not None:
            beta_uncorrected = beta_uncorrected_map.get((phenotype, json_line['gene_set']), '0.0')
            source = source_map[(phenotype, json_line['gene_set'])]
            description, program = gene_set_data_map[json_line["Gene_Set"]]
            return f'{{"gene": "{json_line["Gene"]}", ' \
                   f'"gene_set": "{json_line["gene_set"]}", ' \
                   f'"gene_set_description": "{description}", ' \
                   f'"gene_set_program": "{program}", ' \
                   f'"source": "{source}", ' \
                   f'"prior": {make_option(json_line["prior"])}, ' \
                   f'"combined": {combined}, ' \
                   f'"beta": {beta}, ' \
                   f'"beta_uncorrected": {beta_uncorrected}, ' \
                   f'"log_bf": {make_option(json_line["log_bf"])}, ' \
                   f'"trait_group": "{trait_group}", ' \
                   f'"phenotype": "{phenotype}", ' \
                   f'"gene_set_size": "{gene_set_size}"}}\n'
    return translate_ggss


def translate(trait_group, phenotype, gene_set_size, data_type, file_name, line_fnc):
    download_data(trait_group, phenotype, file_name, gene_set_size)
    with open(f'{data_type}.json', 'w') as f_out:
        with open(file_name, 'r') as f_in:
            header = f_in.readline().strip().split('\t')
            for line in f_in:
                json_line = dict(zip(header, line.strip().split('\t')))
                str_line = line_fnc(json_line, trait_group, phenotype, gene_set_size)
                if str_line is not None:
                    f_out.write(str_line)
    upload_data(trait_group, phenotype, data_type, gene_set_size)
    os.remove(file_name)


def get_ggss_maps(trait_group, phenotype, gene_set_size):
    file_name = 'gss.out'
    beta_uncorrected_map = {}
    source_map = {}
    download_data(trait_group, phenotype, file_name, gene_set_size)
    with open(file_name, 'r') as f_in:
        header = f_in.readline().strip().split('\t')
        for line in f_in:
            json_line = dict(zip(header, line.strip().split('\t')))
            source_map[(phenotype, json_line['Gene_Set'])] = json_line['label']
            beta_uncorrected = make_option(json_line['beta_uncorrected'])
            if beta_uncorrected != 'null':
                beta_uncorrected_map[(phenotype, json_line['Gene_Set'])] = json_line['beta_uncorrected']
    os.remove(file_name)
    return beta_uncorrected_map, source_map


def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--trait-group', default=None, required=True, type=str,
                        help="Input phenotype group.")
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    parser.add_argument('--gene-set-size', default=None, required=True, type=str,
                        help="gene-set-size (small, large, cfde).")
    args = parser.parse_args()

    translate(args.trait_group, args.phenotype, args.gene_set_size, 'gene_stats', 'gs.out', translate_gs)
    translate_gss_func = get_translate_gss()
    translate(args.trait_group, args.phenotype, args.gene_set_size, 'gene_set_stats', 'gss.out', translate_gss_func)
    translate_ggss_func = get_translate_ggss(args.trait_group, args.phenotype, args.gene_set_size)
    translate(args.trait_group, args.phenotype, args.gene_set_size,  'gene_gene_set_stats', 'ggss.out', translate_ggss_func)


if __name__ == '__main__':
    main()
