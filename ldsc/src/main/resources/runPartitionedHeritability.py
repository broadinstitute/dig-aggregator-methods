#!/usr/bin/python3
import argparse
import os
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

downloaded_files = '/mnt/var/ldsc'
ldsc_files = f'{downloaded_files}/ldsc'
baseline_files = f'{downloaded_files}/baseline'
weight_files = f'{downloaded_files}/weights'
frq_files = f'{downloaded_files}/frq'
annot_files = f'{downloaded_files}/annot'

s3_in = 's3://dig-analysis-data'
s3_out = 's3://dig-analysis-data'


def make_path(split_path):
    for i in range(len(split_path)):
        path = './{}'.format('/'.join(split_path[:(i+1)]))
        if not os.path.exists(path):
            os.mkdir(path)


def get_sumstats(ancestry, phenotype):
    file = f'{s3_in}/out/ldsc/sumstats/{phenotype}/ancestry={ancestry}/{phenotype}_{ancestry}.sumstats.gz'
    make_path(['data', 'sumstats', f'{phenotype}', f'ancestry={ancestry}'])
    subprocess.check_call(['aws', 's3', 'cp', file, f'./data/sumstats/{phenotype}/ancestry={ancestry}/'])


def partitioned_heritability(ancestry, phenotype, annot):
    print(f'Partitioned heritability for ancestry: {ancestry}, phenotype: {phenotype}, annot: {annot}')
    g1000_ancestry = ancestry_map[ancestry]
    os.mkdir(f'{ancestry}_{phenotype}_{annot}')
    subprocess.check_call([
        'python3', f'{ldsc_files}/ldsc.py',
        '--h2', f'./data/sumstats/{phenotype}/ancestry={ancestry}/{phenotype}_{ancestry}.sumstats.gz',
        '--ref-ld-chr', f'{baseline_files}/{g1000_ancestry}/baselineLD.,{annot_files}/ancestry={g1000_ancestry}/{annot}/{annot}.',
        '--w-ld-chr', f'{weight_files}/{g1000_ancestry}/weights.',
        '--overlap-annot',
        '--frqfile-chr', f'{frq_files}/{g1000_ancestry}/chr.',
        '--out', f'./{ancestry}_{phenotype}_{annot}/{ancestry}.{phenotype}.{annot}'
    ])


def upload_and_remove_files(ancestry, phenotype, annot):
    s3_dir = f'{s3_out}/out/ldsc/staging/partitioned_heritability/{phenotype}/ancestry={ancestry}/{annot}/'
    subprocess.check_call(['aws', 's3', 'cp', f'./{ancestry}_{phenotype}_{annot}/', s3_dir, '--recursive'])
    shutil.rmtree(f'./{ancestry}_{phenotype}_{annot}')


def run(ancestry, phenotype, annot):
    get_sumstats(ancestry, phenotype)
    partitioned_heritability(ancestry, phenotype, annot)
    upload_and_remove_files(ancestry, phenotype, annot)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', type=str, required=True,
                        help="Phenotype (e.g. T2D)")
    parser.add_argument('--ancestry', type=str, required=True,
                        help="Short ancestry name (e.g. EU)")
    args = parser.parse_args()

    phenotype = args.phenotype
    ancestry = args.ancestry

    if ancestry not in ancestry_map:
        raise Exception(f'Invalid ancestry ({ancestry}), must be one of {", ".join(ancestry_map.keys())}')

    annot = 'accessible_chromatin___central_nervous_system'
    run(ancestry, phenotype, annot)


if __name__ == '__main__':
    main()
