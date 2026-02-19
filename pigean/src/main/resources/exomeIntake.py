#!/usr/bin/python3
import argparse
import gzip
import json
import os
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_data(phenotype):
    path_in = f'{s3_in}/gene_associations/combined/{phenotype}/part-00000.json'
    subprocess.check_call(['aws', 's3', 'cp', path_in, 'data/'])


def convert_file():
    file_out = f'exomes.sumstats.gz'
    with gzip.open(file_out, 'wt') as f_out:
        f_out.write('Gene\tP-value\tEffect\n')
        with open('./data/part-00000.json', 'r') as f_in:
            for line in f_in:
                json_line = json.loads(line)
                if type(json_line['gene']) == str and json_line['gene'] != '':
                    f_out.write('\t'.join([
                        json_line['gene'],
                        str(json_line['pValue']),
                        str(json_line['beta'])
                    ]) + '\n')
    os.remove('./data/part-00000.json')


def upload(phenotype):
    file_out = 'exomes.sumstats.gz'
    subprocess.check_call(['aws', 's3', 'cp', file_out, f'{s3_out}/out/pigean/inputs/exomes/portal_exomes/exomes_{phenotype}/'])
    os.remove(file_out)


def success(phenotype):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', f'{s3_out}/out/pigean/inputs/exomes/portal_exomes/exomes_{phenotype}/'])
    os.remove('_SUCCESS')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    args = parser.parse_args()
    download_data(args.phenotype)
    convert_file()
    upload(args.phenotype)
    success(args.phenotype)


if __name__ == '__main__':
    main()
