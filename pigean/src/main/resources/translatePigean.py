#!/usr/bin/python3
import argparse
import os
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_data(phenotype, file_name):
    file_path = f'{s3_in}/out/pigean/staging/pigean/{phenotype}/{file_name}'
    subprocess.check_call(['aws', 's3', 'cp', file_path, '.'])


def upload_data(phenotype, data_type):
    file_path = f'{s3_out}/out/pigean/{data_type}/{phenotype}/'
    file_out = f'{phenotype}.{data_type}.json'
    subprocess.check_call(['aws', 's3', 'cp', file_out, file_path])
    success(file_path)
    os.remove(file_out)


def make_option(value):
    return value if value != 'NA' else 'null'


def translate_gs(json_line, phenotype):
    combined = make_option(json_line["combined"])
    if combined is not None:
        return f'{{"gene": "{json_line["Gene"]}", ' \
               f'"prior": {make_option(json_line["prior"])}, ' \
               f'"combined": {combined}, ' \
               f'"huge_score": {make_option(json_line["huge_score_gwas"])}, ' \
               f'"log_bf": {make_option(json_line["log_bf"])}, ' \
               f'"n": {make_option(json_line["N"])}, ' \
               f'"phenotype": "{phenotype}"}}\n'


def translate_gss(json_line, phenotype):
    beta = make_option(json_line["beta"])
    beta_uncorrected = make_option(json_line["beta_uncorrected"])
    if beta is not None and beta_uncorrected is not None and float(beta_uncorrected) != 0.0:
        return f'{{"gene_set": "{json_line["Gene_Set"]}", ' \
               f'"source": "{json_line["label"]}", ' \
               f'"beta": {beta}, ' \
               f'"beta_uncorrected": {beta_uncorrected}, ' \
               f'"n": {make_option(json_line["N"])}, ' \
               f'"phenotype": "{phenotype}"}}\n'


def translate_ggss(json_line, phenotype):
    beta = make_option(json_line["beta"])
    combined = make_option(json_line["combined"])
    if beta is not None and combined is not None:
        return f'{{"gene": "{json_line["Gene"]}", ' \
               f'"gene_set": "{json_line["gene_set"]}", ' \
               f'"prior": {make_option(json_line["prior"])}, ' \
               f'"combined": {combined}, ' \
               f'"beta": {beta}, ' \
               f'"log_bf": {make_option(json_line["log_bf"])}, ' \
               f'"phenotype": "{phenotype}"}}\n'


def translate(phenotype, data_type, file_name, line_fnc):
    download_data(phenotype, file_name)
    with open(f'{phenotype}.{data_type}.json', 'w') as f_out:
        with open(file_name, 'r') as f_in:
            header = f_in.readline().strip().split('\t')
            for line in f_in:
                json_line = dict(zip(header, line.strip().split('\t')))
                str_line = line_fnc(json_line, phenotype)
                if str_line is not None:
                    f_out.write(str_line)
    upload_data(phenotype, data_type)
    os.remove(file_name)


def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    args = parser.parse_args()

    translate(args.phenotype, 'gene_stats', 'gs.out', translate_gs)
    translate(args.phenotype,'gene_set_stats', 'gss.out', translate_gss)
    translate(args.phenotype, 'gene_gene_set_stats', 'ggss.out', translate_ggss)


if __name__ == '__main__':
    main()
