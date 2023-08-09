#!/usr/bin/python3
import argparse
import glob
import os
import subprocess

s3_dir = 's3://dig-analysis-pxs'

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
sumstat_files = f'{downloaded_files}/sumstats'
ldscore_files = f'{downloaded_files}/ldscore'

s3_path = f'{s3_dir}/out/ldsc/staging/genetic_correlation'


def run_all(ancestry, phenotype, all_files):
    subprocess.check_call([
        'python3', f'{ldsc_files}/ldsc.py',
        '--rg', all_files,
        '--ref-ld-chr', f'{ldscore_files}/{ancestry_map[ancestry]}/chr@',
        '--w-ld-chr', f'{ldscore_files}/{ancestry_map[ancestry]}/chr@',
        '--out', f'./{phenotype}_{ancestry}'
    ])


def upload_and_remove_output(ancestry, phenotype):
    file = f'./{phenotype}_{ancestry}.log'
    subprocess.check_call(['aws', 's3', 'cp', file, f'{s3_path}/ancestry={ancestry}/'])
    os.remove(file)
    if os.path.exists(f'{phenotype}_{ancestry}.sumstats.gz'):
        os.remove(f'{phenotype}_{ancestry}.sumstats.gz')


# This allows for other buckets to run against the data in dig-analysis-data (downloaded in downloadSumstatsFiles.py)
def check_and_download_if_missing(phenotype, ancestry):
    main_file = f'{sumstat_files}/{ancestry}/{phenotype}_{ancestry}.sumstats.gz'
    if not os.path.exists(main_file):
        file = f'{s3_dir}/out/ldsc/sumstats/{phenotype}/ancestry={ancestry}/{phenotype}_{ancestry}.sumstats.gz'
        subprocess.check_call(['aws', 's3', 'cp', file, f'{phenotype}_{ancestry}.sumstats.gz'])
        return f'{phenotype}_{ancestry}.sumstats.gz'
    else:
        return main_file


def run(ancestry, phenotype):
    main_file = check_and_download_if_missing(phenotype, ancestry)
    all_sumstats = glob.glob(f'{sumstat_files}/{ancestry}/*')
    other_files = [other_file for other_file in all_sumstats if other_file != main_file]
    run_all(ancestry, phenotype, f'{main_file},{",".join(other_files)}')
    upload_and_remove_output(ancestry, phenotype)


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

    run(ancestry, phenotype)


if __name__ == '__main__':
    main()
