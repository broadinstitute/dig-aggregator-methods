#!/usr/bin/python3
import argparse
from boto3.session import Session
import json
import os
import subprocess

downloaded_files = '/mnt/var/pigean'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_data(trait_group, phenotype, sigma, gene_set_size):
    file_path = f'{s3_in}/out/pigean/staging/factor/{trait_group}/{phenotype}/sigma={sigma}/size={gene_set_size}'
    subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/f.out', '.'])
    subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/gc.out', '.'])


def run_graph():
    cmd = [
              'python3', f'{downloaded_files}/factor_graph.py',
              '--gene-factors-in', 'gc.out',
              '--factors-in', 'f.out',
              '--json-out', 'graph.json'
        ]
    subprocess.check_call(cmd)


def add_fields(trait_group, phenotype, sigma, gene_set_size):
    file = f'{phenotype}.{sigma}.{gene_set_size}.graph.json'
    with open(file, 'w') as f_out:
        with open('graph.json', 'r') as f_in:
            data = json.load(f_in)
        data['trait_group'] = trait_group
        data['phenotype'] = phenotype
        data['sigma'] = sigma
        data['gene_set_size'] = gene_set_size
        json.dump(data, f_out)
    return file


def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def upload_data(file, trait_group, phenotype, sigma, gene_set_size):
    file_path = f'{s3_out}/out/pigean/graph/{trait_group}/sigma={sigma}/size={gene_set_size}/{phenotype}/'
    subprocess.check_call(['aws', 's3', 'cp', file, file_path])
    os.remove(file)
    success(file_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--trait-group', default=None, required=True, type=str,
                        help="Input phenotype group.")
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    parser.add_argument('--sigma', default=None, required=True, type=str,
                        help="Sigma power (0, 2, 4).")
    parser.add_argument('--gene-set-size', default=None, required=True, type=str,
                        help="gene-set-size (small, medium, or large).")
    args = parser.parse_args()

    download_data(args.trait_group, args.phenotype, args.sigma, args.gene_set_size)
    try:
        run_graph()
        file = add_fields(args.trait_group, args.phenotype, args.sigma, args.gene_set_size)
        upload_data(file, args.trait_group, args.phenotype, args.sigma, args.gene_set_size)
    except Exception:
        print('Error')
    os.remove('gc.out')
    os.remove('f.out')


if __name__ == '__main__':
    main()
