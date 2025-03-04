#!/usr/bin/python3
import argparse
import os
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_data(trait_group, phenotype, file_name, sigma, gene_set_size):
    file_path = f'{s3_in}/out/pigean/staging/phewas/{trait_group}/{phenotype}/sigma={sigma}/size={gene_set_size}/{file_name}'
    subprocess.check_call(['aws', 's3', 'cp', file_path, '.'])


def upload_data(trait_group, phenotype, data_type, sigma, gene_set_size):
    file_path = f'{s3_out}/out/pigean/{data_type}/sigma={sigma}/size={gene_set_size}/{trait_group}/{phenotype}/'
    subprocess.check_call(['aws', 's3', 'cp', 'phewas.json', file_path])
    success(file_path)
    os.remove('phewas.json')


def get_pz(json_line):
    binary_vals = (float(json_line['P']), float(json_line['Z']))
    robust_vals = (float(json_line['P_robust']), float(json_line['Z_robust']))
    return max([binary_vals, robust_vals])


def translate_phewas(json_line, phenotype, sigma, gene_set_size):
    if json_line["Pheno"] != phenotype:
        p, z = get_pz(json_line)
        # convert p to one-sided p
        return f'{{"factor": "{json_line["Factor"]}", ' \
               f'"other_phenotype": "{json_line["Pheno"]}", ' \
               f'"pValue": {p / 2 if z > 0 else 1 - p / 2}, ' \
               f'"pValue_orig": {json_line["P"]}, ' \
               f'"pValue_marginal": {json_line["P_marg"]}, ' \
               f'"pValue_robust": {json_line["P_robust"]}, ' \
               f'"Z": {z}, ' \
               f'"phenotype": "{phenotype}", ' \
               f'"sigma": {sigma}, ' \
               f'"gene_set_size": "{gene_set_size}"}}\n'


def translate(trait_group, phenotype, sigma, gene_set_size, data_type, file_name, line_fnc):
    download_data(trait_group, phenotype, file_name, sigma, gene_set_size)
    with open('phewas.json', 'w') as f_out:
        with open(file_name, 'r') as f_in:
            header = f_in.readline().strip().split('\t')
            for line in f_in:
                json_line = dict(zip(header, line.strip().split('\t')))
                str_line = line_fnc(json_line, phenotype, sigma, gene_set_size)
                if str_line is not None:
                    f_out.write(str_line)
    upload_data(trait_group, phenotype, data_type, sigma, gene_set_size)
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
    parser.add_argument('--sigma', default=None, required=True, type=str,
                        help="Sigma")
    parser.add_argument('--gene-set-size', default=None, required=True, type=str,
                        help="Gene Set Size (small, medium, large)")
    args = parser.parse_args()

    translate(args.trait_group, args.phenotype, args.sigma, args.gene_set_size, 'phewas', 'phewas.out', translate_phewas)


if __name__ == '__main__':
    main()
