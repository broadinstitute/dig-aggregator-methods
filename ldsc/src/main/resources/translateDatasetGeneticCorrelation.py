#!/usr/bin/python3
import argparse
import glob
import os
import subprocess


downloaded_files = '/mnt/var/ldsc'

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_staging(phenotype, ancestry):
    subprocess.check_call(['aws', 's3', 'cp',
                           f'{s3_in}/out/ldsc/staging/dataset_genetic_correlation/{phenotype}/{ancestry}',
                           f'gc/',
                           '--recursive'])
    subprocess.check_call(['aws', 's3', 'cp',
                           f'{s3_in}/out/ldsc/staging/dataset_heritability/{phenotype}/{ancestry}',
                           f'heritability/',
                           '--recursive'])

def combine():
    pass


def upload_and_remove_output(phenotype, ancestry):
    f_out = f'{s3_out}/out/ldsc/dataset_gcov/{phenotype}/{ancestry}'
    subprocess.check_call(['aws', 's3', 'cp', 'gcov.json', f'{f_out}/'])
    subprocess.check_call(['aws', 's3', 'cp', 'gcov_int.json', f'{f_out}/'])
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', f'{f_out}/_SUCCESS'])
    os.remove('gcov.json')
    os.remove('gcov_int.json')
    os.remove('_SUCCESS')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Primary phenotype.")
    parser.add_argument('--ancestry', default=None, required=True, type=str,
                        help="Ancestry, should be two letter version (e.g. EU) and will be made upper.")
    args = parser.parse_args()
    phenotype = args.phenotype
    ancestry = args.ancestry

    download_staging(phenotype, ancestry)
    combine()
    upload_and_remove_output(phenotype, ancestry)


if __name__ == '__main__':
    main()
