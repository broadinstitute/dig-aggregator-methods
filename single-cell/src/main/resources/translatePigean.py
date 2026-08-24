#!/usr/bin/python3
import argparse
import glob
import os
import re
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_data(dataset, cell_type, model):
    file_path = f'{s3_in}/out/single_cell/staging/pigean/{dataset}/{cell_type}/{model}/'
    subprocess.check_call(['aws', 's3', 'cp', file_path, 'inputs/', '--recursive'])


def upload_data(dataset, cell_type, model):
    file_path = f'{s3_out}/out/single_cell/pigean/{dataset}/{cell_type}/{model}/'
    subprocess.check_call(['aws', 's3', 'cp', 'outputs/gene_set_stats.json', file_path])
    success(file_path)


def make_option(value):
    return value if value != 'NA' else 'null'


def translate_gss_line(json_line, factor, dataset, cell_type, model):
    beta = make_option(json_line["beta"])
    beta_uncorrected = make_option(json_line["beta_uncorrected"])
    if beta is not None and beta_uncorrected is not None and float(beta_uncorrected) != 0.0:
        return f'{{"factor": "{factor}", ' \
               f'"gene_set": "{json_line["Gene_Set"]}", ' \
               f'"source": "{json_line["label"]}", ' \
               f'"beta": {beta}, ' \
               f'"beta_uncorrected": {beta_uncorrected}, ' \
               f'"n": {make_option(json_line["N"])}, ' \
               f'"dataset": "{dataset}", ' \
               f'"cell_type": "{cell_type}", ' \
               f'"model": "{model}"}}\n'


def translate_gss(dataset, cell_type, model):
    with open('outputs/gene_set_stats.json', 'w') as f_out:
        for file in glob.glob('inputs/gss.*.out'):
            factor = re.findall(r'inputs/gss\.(.*)\.out', file)[0]
            with open(file, 'r') as f_in:
                header = f_in.readline().strip().split('\t')
                for line in f_in:
                    json_line = dict(zip(header, line.strip().split('\t')))
                    str_line = translate_gss_line(json_line, factor, dataset, cell_type, model)
                    if str_line is not None:
                        f_out.write(str_line)


def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default=None, required=True, type=str)
    parser.add_argument('--cell-type', default=None, required=True, type=str)
    parser.add_argument('--model', default=None, required=True, type=str)
    args = parser.parse_args()

    download_data(args.dataset, args.cell_type, args.model)
    os.makedirs('outputs', exist_ok=True)
    translate_gss(args.dataset, args.cell_type, args.model)
    upload_data(args.dataset, args.cell_type, args.model)
    shutil.rmtree('inputs')
    shutil.rmtree('outputs')

if __name__ == '__main__':
    main()
