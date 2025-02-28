#!/usr/bin/python3
import argparse
import glob
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
sumstat_files = f'{downloaded_files}/sumstats'
ldscore_files = f'{downloaded_files}/ldscore'

project = os.environ['PROJECT']
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def run_all(ancestry, phenotype, all_files):
    subprocess.check_call([
        'python3', f'{ldsc_files}/ldsc.py',
        '--rg', all_files,
        '--ref-ld-chr', f'{ldscore_files}/{ancestry_map[ancestry]}/chr@',
        '--w-ld-chr', f'{ldscore_files}/{ancestry_map[ancestry]}/chr@',
        '--out', f'./{phenotype}_{ancestry}'
    ])


def upload_and_remove_output(ancestry, phenotype, other_project):
    f_in = f'./h2.log'
    f_out = f'{s3_out}/out/ldsc/staging/genetic_correlation/ancestry={ancestry}/{other_project}'
    subprocess.check_call(['aws', 's3', 'cp', f_in, f'{f_out}/{phenotype}_{ancestry}.log'])
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', f'{f_out}/_SUCCESS'])
    os.remove(f_in)
    os.remove('_SUCCESS')


def run_project(ancestry, phenotype, main_file, other_project):
    all_files = glob.glob(f'{sumstat_files}/{other_project}/*/ancestry={ancestry}/*.sumstats.gz')
    all_files_but_main = [other_file for other_file in all_files if other_file != main_file]
    run_all(ancestry, phenotype, f'{main_file},{",".join(all_files_but_main)}')
    upload_and_remove_output(ancestry, phenotype, other_project)


def run(ancestry, phenotype):
    main_file = f'{sumstat_files}/{project}/{phenotype}/ancestry={ancestry}/{phenotype}_{ancestry}.sumstats.gz'
    if os.path.exists(main_file):
        for other_project in set([project]):
            run_project(ancestry, phenotype, main_file, other_project)


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
