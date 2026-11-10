#!/usr/bin/python3
import argparse
import os
import subprocess

ancestry_map = {
    'AA': 'AFR',
    'AF': 'AFR',
    'SSAF': 'AFR',
    'HS': 'AMR',
    'EA': 'EAS',
    'EU': 'EUR',
    'SA': 'SAS',
    'GME': 'SAS',
    'Mixed': 'EUR'
}

downloaded_files = '/mnt/var/ldsc'
ldsc_files = f'{downloaded_files}/ldsc'
ldscore_files = f'{downloaded_files}/ldscore'

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_file(phenotype, ancestry, dataset):
    path_in = f'{s3_in}/out/ldsc/dataset_sumstats/{phenotype}/{ancestry}/{dataset}'
    subprocess.check_call(['aws', 's3', 'cp', f'{path_in}/ldsc.sumstats.gz', '.'])


def run_heritability(ancestry):
    subprocess.check_call([
        'python3', f'{ldsc_files}/ldsc.py',
        '--h2', 'ldsc.sumstats.gz',
        '--ref-ld-chr', f'{ldscore_files}/{ancestry_map[ancestry]}/chr@',
        '--w-ld-chr', f'{ldscore_files}/{ancestry_map[ancestry]}/chr@',
        '--out', 'outputs'
    ])


def upload_and_remove_output(phenotype, ancestry, dataset):
    f_out = f'{s3_out}/out/ldsc/staging/dataset_heritability/{phenotype}/{ancestry}/{dataset}'
    subprocess.check_call(['aws', 's3', 'cp', 'h2.log', f'{f_out}/h2.log'])
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', f'{f_out}/_SUCCESS'])
    os.remove('h2.log')
    os.remove('_SUCCESS')
    os.remove('ldsc.sumstats.gz')


def run(phenotype, ancestry, dataset):
    download_file(phenotype, ancestry, dataset)
    run_heritability(ancestry)
    upload_and_remove_output(phenotype, ancestry, dataset)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Primary phenotype.")
    parser.add_argument('--ancestry', default=None, required=True, type=str,
                        help="Ancestry, should be two letter version (e.g. EU) and will be made upper.")
    parser.add_argument('--dataset', default=None, required=True, type=str,
                        help="Dataset ID")
    args = parser.parse_args()
    phenotype = args.phenotype
    ancestry = args.ancestry
    dataset = args.dataset
    if ancestry not in ancestry_map:
        raise Exception(f'Invalid ancestry ({ancestry}), must be one of {", ".join(ancestry_map.keys())}')

    run(phenotype, ancestry, dataset)


if __name__ == '__main__':
    main()
