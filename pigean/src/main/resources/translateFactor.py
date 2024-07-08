#!/usr/bin/python3
import argparse
import os
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_data(phenotype, file_name, sigma, gene_set_size):
    file_path = f'{s3_in}/out/pigean/staging/factor/{phenotype}/sigma={sigma}/size={gene_set_size}/{file_name}'
    subprocess.check_call(['aws', 's3', 'cp', file_path, '.'])


def upload_data(phenotype, data_type, sigma, gene_set_size):
    file_path = f'{s3_out}/out/pigean/{data_type}/sigma={sigma}/size={gene_set_size}/{phenotype}/'
    file_out = f'{phenotype}.{sigma}.{gene_set_size}.{data_type}.json'
    subprocess.check_call(['aws', 's3', 'cp', file_out, file_path])
    success(file_path)
    os.remove(file_out)


def translate_f(json_line, phenotype, sigma, gene_set_size):
    return f'{{"cluster": "{json_line["Factor"]}", ' \
           f'"label": "{json_line["label"]}", ' \
           f'"top_genes": "{json_line["top_genes"].replace(",", ";")}", ' \
           f'"top_gene_sets": "{json_line["top_gene_sets"].replace(",", ";")}", ' \
           f'"gene_score": {json_line["gene_score"]}, ' \
           f'"gene_set_score": {json_line["gene_set_score"]}, ' \
           f'"phenotype": "{phenotype}", ' \
           f'"sigma": {sigma}, ' \
           f'"gene_set_size": "{gene_set_size}"}}\n'


def translate_gc(json_line, phenotype, sigma, gene_set_size):
    return f'{{"gene": "{json_line["Gene"]}", ' \
           f'"label": "{json_line["label"]}", ' \
           f'"factor": "{json_line["cluster"]}", ' \
           f'"factor_value": {json_line[json_line["cluster"]]}, ' \
           f'"prior": {json_line["prior"]}, ' \
           f'"combined": {json_line["prior"]}, ' \
           f'"log_bf": {json_line["log_bf"]}, ' \
           f'"phenotype": "{phenotype}", ' \
           f'"sigma": {sigma}, ' \
           f'"gene_set_size": "{gene_set_size}"}}\n'


def translate_gsc(json_line, phenotype, sigma, gene_set_size):
    return f'{{"gene_set": "{json_line["Gene_Set"]}", ' \
           f'"label": "{json_line["label"]}", ' \
           f'"factor": "{json_line["cluster"]}", ' \
           f'"factor_value": {json_line[json_line["cluster"]]}, ' \
           f'"beta": {json_line["beta"]}, ' \
           f'"beta_uncorrected": {json_line["beta_uncorrected"]}, ' \
           f'"phenotype": "{phenotype}", ' \
           f'"sigma": {sigma}, ' \
           f'"gene_set_size": "{gene_set_size}"}}\n'


def translate(phenotype, sigma, gene_set_size, data_type, file_name, line_fnc):
    download_data(phenotype, file_name, sigma, gene_set_size)
    with open(f'{phenotype}.{sigma}.{gene_set_size}.{data_type}.json', 'w') as f_out:
        with open(file_name, 'r') as f_in:
            header = f_in.readline().strip().split('\t')
            for line in f_in:
                json_line = dict(zip(header, line.strip().split('\t')))
                str_line = line_fnc(json_line, phenotype, sigma, gene_set_size)
                if str_line is not None:
                    f_out.write(str_line)
    upload_data(phenotype, data_type, sigma, gene_set_size)
    os.remove(file_name)


def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    parser.add_argument('--sigma', default=None, required=True, type=str,
                        help="Sigma")
    parser.add_argument('--gene-set-size', default=None, required=True, type=str,
                        help="Gene Set Size (small, medium, large)")
    args = parser.parse_args()

    translate(args.phenotype, args.sigma, args.gene_set_size, 'factor', 'f.out', translate_f)
    translate(args.phenotype, args.sigma, args.gene_set_size, 'gene_factor', 'gc.out', translate_gc)
    translate(args.phenotype, args.sigma, args.gene_set_size, 'gene_set_factor', 'gsc.out', translate_gsc)


if __name__ == '__main__':
    main()
