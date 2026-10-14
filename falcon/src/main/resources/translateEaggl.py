#!/usr/bin/python3
import argparse
import gzip
import os
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


# Temp bodge
idxs = {'gene_clusters.out.gz': 7, 'gene_set_clusters.out,gz': 6}
def get_factors(filename):
    if filename in idxs:
        with gzip.open(filename, 'rt') as f:
            return f.readline().strip().split('\t')[idxs[filename]:]
    else:
        return []


def download_data(trait_group, phenotype, file_name, model):
    file_path = f'{s3_in}/out/falcon/staging/eaggl/{trait_group}/{phenotype}/{model}/{file_name}'
    subprocess.check_call(['aws', 's3', 'cp', file_path, '.'])


def upload_data(trait_group, phenotype, data_type, model):
    file_path = f'{s3_out}/out/falcon/{data_type}/{model}/{trait_group}/{phenotype}/'
    file_out = f'{data_type}.json'
    subprocess.check_call(['aws', 's3', 'cp', file_out, file_path])
    success(file_path)
    os.remove(file_out)


def translate_f(json_line, trait_group, phenotype, model, factors):
    return [
        f'{{"factor": "{json_line["Factor"]}", '
        f'"label": "{json_line["label"]}", '
        f'"top_genes": "{json_line["top_genes"].replace(",", ";")}", '
        f'"top_gene_sets": "{json_line["top_gene_sets"].replace(",", ";")}", '
        f'"lambda": {json_line["lambda"]}, '
        f'"anchor_any_joint": {json_line["anchor_any_joint"]}, '
        f'"anchor_any_marginal": {json_line["anchor_any_marginal"]}, '
        f'"trait_group": "{trait_group}", '
        f'"phenotype": "{phenotype}", '
        f'"gene_set_size": "{model}", ' \
        f'"sigma": 2}}\n'
    ]


def translate_gc(json_line, trait_group, phenotype, model, factors):
    out = []
    for factor in factors:
        if float(json_line[factor]) > 0:
            out.append(f'{{"gene": "{json_line["Gene"]}", '
                       f'"label_factor": "{json_line["cluster"]}", '
                       f'"label": "{json_line["label"]}", '
                       f'"factor": "{factor}", '
                       f'"factor_value": {json_line[factor]}, '
                       f'"prior": {json_line["prior"]}, '
                       f'"combined": {json_line["combined"]}, '
                       f'"log_bf": {json_line["log_bf"]}, '
                       f'"trait_group": "{trait_group}", '
                       f'"phenotype": "{phenotype}", '
                       f'"gene_set_size": "{model}", '
                       f'"sigma": 2}}\n')
    return out


def translate_gsc(json_line, trait_group, phenotype, model, factors):
    out = []
    for factor in factors:
        if float(json_line[factor]) > 0:
            out.append(f'{{"gene_set": "{json_line["Gene_Set"]}", '
                       f'"label_factor": "{json_line["cluster"]}", '
                       f'"label": "{json_line["label"]}", '
                       f'"factor": "{factor}", '
                       f'"factor_value": {json_line[factor]}, '
                       f'"beta": {json_line["beta"]}, '
                       f'"beta_uncorrected": {json_line["beta_uncorrected"]}, '
                       f'"trait_group": "{trait_group}", '
                       f'"phenotype": "{phenotype}", '
                       f'"gene_set_size": "{model}", '
                       f'"sigma": 2}}\n')
    return out


def translate(trait_group, phenotype, model, data_type, file_name, line_fnc):
    download_data(trait_group, phenotype, file_name, model)
    factors = get_factors(file_name)
    with open(f'{data_type}.json', 'w') as f_out:
        with gzip.open(file_name, 'rt') as f_in:
            header = f_in.readline().strip().split('\t')
            for line in f_in:
                json_line = dict(zip(header, line.strip().split('\t')))
                str_lines = line_fnc(json_line, trait_group, phenotype, model, factors)
                for str_line in str_lines:
                    f_out.write(str_line)
    upload_data(trait_group, phenotype, data_type, model)
    os.remove(file_name)


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
    parser.add_argument('--model', default=None, required=True, type=str,
                        help="model (e.g. mouse_msigdb).")
    args = parser.parse_args()

    translate(args.trait_group, args.phenotype, args.model, 'factor', 'factors.out.gz', translate_f)
    translate(args.trait_group, args.phenotype, args.model, 'gene_factor', 'gene_clusters.out.gz', translate_gc)
    translate(args.trait_group, args.phenotype, args.model, 'gene_set_factor', 'gene_set_clusters.out.gz', translate_gsc)


if __name__ == '__main__':
    main()
