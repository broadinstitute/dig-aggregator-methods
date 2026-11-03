#!/usr/bin/python3
import argparse
import glob
import os
import re
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


def download_sumstats(phenotype, ancestry):
    subprocess.check_call(['aws', 's3', 'cp',
                           f'{s3_in}/out/ldsc/dataset_sumstats/{phenotype}/{ancestry}/',
                           f'sumstats/{phenotype}/{ancestry}/',
                           '--recursive'])
    files = glob.glob(f'sumstats/{phenotype}/{ancestry}/*/ldsc.sumstats.gz')
    pattern = r'sumstats/{}/{}/([^/]*)/ldsc.sumstats.gz'.format(phenotype, ancestry)
    return list({dataset for file in files for dataset in re.findall(pattern, file)})


def run_all(phenotype, ancestry, dataset, all_files):
    subprocess.check_call([
        'python3', f'{ldsc_files}/ldsc.py',
        '--rg', all_files,
        '--ref-ld-chr', f'{ldscore_files}/{ancestry_map[ancestry]}/chr@',
        '--w-ld-chr', f'{ldscore_files}/{ancestry_map[ancestry]}/chr@',
        '--out', f'./{phenotype}_{ancestry}_{dataset}'
    ])


def upload_and_remove_output(phenotype, ancestry, dataset):
    f_in = f'./h2.log'
    f_out = f'{s3_out}/out/ldsc/staging/dataset_genetic_correlation/{phenotype}/{ancestry}'
    subprocess.check_call(['aws', 's3', 'cp', f_in, f'{f_out}/{dataset}.log'])
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', f'{f_out}/_SUCCESS'])
    os.remove(f_in)
    os.remove('_SUCCESS')


def run_project(phenotype, ancestry, dataset, main_file):
    all_files = glob.glob(f'sumstats/{phenotype}/{ancestry}/*/ldsc.sumstats.gz')
    all_files_but_main = [other_file for other_file in all_files if other_file != main_file]
    run_all(phenotype, ancestry, dataset, f'{main_file},{",".join(all_files_but_main)}')
    upload_and_remove_output(phenotype, ancestry, dataset)


def run(phenotype, ancestry, dataset):
    main_file = f'sumstats/{phenotype}/{ancestry}/{dataset}/ldsc.sumstats.gz'
    if os.path.exists(main_file):
        run_project(phenotype, ancestry, dataset, main_file)


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

    for dataset in download_sumstats(phenotype, ancestry):
        run(phenotype, ancestry, dataset)


if __name__ == '__main__':
    main()
