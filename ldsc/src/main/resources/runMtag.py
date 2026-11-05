#!/usr/bin/python3
import argparse
import glob
import os
import re
import shutil
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

downloaded_files = '/mnt/var/mtag'

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_sumstats(phenotype, ancestry):
    subprocess.check_call(['aws', 's3', 'cp',
                           f'{s3_in}/out/ldsc/dataset_sumstats/{phenotype}/{ancestry}/',
                           f'sumstats/{phenotype}/{ancestry}/',
                           '--recursive'])
    files = glob.glob(f'sumstats/{phenotype}/{ancestry}/*/ldsc.sumstats.gz')
    pattern = r'sumstats/{}/{}/([^/]*)/ldsc.sumstats.gz'.format(phenotype, ancestry)
    return list({dataset for file in files for dataset in re.findall(pattern, file)})


def run_mtag(files):
    subprocess.check_call([
        'python3.11', f'{downloaded_files}/mtag/mtag.py'
        '--sumstats', ','.join(files),
        '--snp_name', 'SNP',
        '--a1_name', 'A1',
        '--a2_name', 'A2',
        '--z_name', 'Z',
        '--n_name', 'N',
        '--eaf_name', 'FRQ',
        '--p_name', 'P',
        '--no_chr_data'
        '--ld_ref_panel', f'{downloaded_files}/ldscore/EUR/chr',
        '--out', 'outputs/mtag'
    ])


def upload_and_remove_output(phenotype, ancestry):
    f_out = f'{s3_out}/out/ldsc/staging/dataset_mtag/{phenotype}/{ancestry}'
    subprocess.check_call(['aws', 's3', 'rm', f'{f_out}/', '--recursive'])
    subprocess.check_call(['aws', 's3', 'cp', 'outputs/', f'{f_out}/', '--recursive'])
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', f'{f_out}/_SUCCESS'])
    shutil.rmtree('outputs')
    os.remove('_SUCCESS')


def run(phenotype, ancestry):
    files = glob.glob(f'sumstats/{phenotype}/{ancestry}/*/ldsc.sumstats.gz')
    run_mtag(files)
    upload_and_remove_output(phenotype, ancestry)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Primary phenotype.")
    parser.add_argument('--ancestry', default=None, required=True, type=str,
                        help="Ancestry, should be two letter version (e.g. EU) and will be made upper.")
    args = parser.parse_args()
    phenotype = args.phenotype
    ancestry = args.ancestry
    if ancestry not in ancestry_map:
        raise Exception(f'Invalid ancestry ({ancestry}), must be one of {", ".join(ancestry_map.keys())}')

    download_sumstats(phenotype, ancestry)
    run(phenotype, ancestry)


if __name__ == '__main__':
    main()
