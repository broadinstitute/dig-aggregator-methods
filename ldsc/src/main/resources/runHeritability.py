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


def download_file(ancestry, phenotype):
    path_in = f'{s3_in}/out/ldsc/sumstats/{phenotype}/ancestry={ancestry}'
    subprocess.check_call(['aws', 's3', 'cp', f'{path_in}/{phenotype}_{ancestry}.sumstats.gz', '.'])


def run_heritability(ancestry, phenotype):
    subprocess.check_call([
        'python3', f'{ldsc_files}/ldsc.py',
        '--h2', f'{phenotype}_{ancestry}.sumstats.gz',
        '--ref-ld-chr', f'{ldscore_files}/{ancestry_map[ancestry]}/chr@',
        '--w-ld-chr', f'{ldscore_files}/{ancestry_map[ancestry]}/chr@',
        '--out', f'./{phenotype}_{ancestry}'
    ])


def upload_and_remove_output(ancestry, phenotype):
    f_in = f'./h2.log'
    f_out = f'{s3_out}/out/ldsc/staging/heritability/{phenotype}/ancestry={ancestry}'
    subprocess.check_call(['aws', 's3', 'cp', f_in, f'{f_out}/{phenotype}_{ancestry}.log'])
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', f'{f_out}/_SUCCESS'])
    os.remove(f_in)
    os.remove('_SUCCESS')
    os.remove(f'{phenotype}_{ancestry}.sumstats.gz')


def run(ancestry, phenotype):
    download_file(ancestry, phenotype)
    run_heritability(ancestry, phenotype)
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
