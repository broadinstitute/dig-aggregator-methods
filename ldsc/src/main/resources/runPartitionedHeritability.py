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

downloaded_files = '/mnt/var/ldsc'
ldsc_files = f'{downloaded_files}/ldsc'
baseline_files = f'{downloaded_files}/baseline'
weight_files = f'{downloaded_files}/weights'
frq_files = f'{downloaded_files}/frq'
annot_files = f'{downloaded_files}/annot'

s3_in = 's3://dig-analysis-data'
s3_out = 's3://dig-analysis-data'


def get_all_annot_basenames(ancestry):
    pattern = f'{annot_files}/ancestry={ancestry_map[ancestry]}/*/*.1.annot.gz'
    return [re.findall('.*/([^/]*).1.annot.gz', file)[0] for file in glob.glob(pattern)]


def make_path(split_path):
    for i in range(len(split_path)):
        path = './{}'.format('/'.join(split_path[:(i+1)]))
        if not os.path.exists(path):
            os.mkdir(path)


def get_sumstats(ancestry, phenotype):
    file = f'{s3_in}/out/ldsc/sumstats/{phenotype}/ancestry={ancestry}/{phenotype}_{ancestry}.sumstats.gz'
    make_path(['data', 'sumstats', f'{phenotype}', f'ancestry={ancestry}'])
    out = subprocess.run(['aws', 's3', 'cp', file, f'./data/sumstats/{phenotype}/ancestry={ancestry}/'])
    return out.returncode == 0


def partitioned_heritability(ancestry, phenotype, annots):
    print(f'Partitioned heritability for ancestry: {ancestry}, phenotype: {phenotype}, annots: {annots}')
    g1000_ancestry = ancestry_map[ancestry]
    annot_str = ','.join([f'{annot_files}/ancestry={g1000_ancestry}/{annot}/{annot}.' for annot in annots])
    os.mkdir(f'{ancestry}_{phenotype}')
    subprocess.check_call([
        'python3', f'{ldsc_files}/ldsc.py',
        '--h2', f'./data/sumstats/{phenotype}/ancestry={ancestry}/{phenotype}_{ancestry}.sumstats.gz',
        '--ref-ld-chr', f'{baseline_files}/{g1000_ancestry}/baselineLD.,{annot_str}',
        '--h2-split-annot', '--h2-threads', '22',
        '--w-ld-chr', f'{weight_files}/{g1000_ancestry}/weights.',
        '--overlap-annot', '--print-coefficients',
        '--frqfile-chr', f'{frq_files}/{g1000_ancestry}/chr.',
        '--out', f'./{ancestry}_{phenotype}/{ancestry}.{phenotype}'
    ])


def upload_and_remove_files(ancestry, phenotype):
    s3_dir = f'{s3_out}/out/ldsc/staging/partitioned_heritability/{phenotype}/ancestry={ancestry}/'
    subprocess.check_call(['aws', 's3', 'cp', f'./{ancestry}_{phenotype}/', s3_dir, '--recursive'])
    shutil.rmtree(f'./{ancestry}_{phenotype}')


# Need to check on sumstats existence for Mixed ancestry datasets
def run(ancestry, phenotype, annots):
    if get_sumstats(ancestry, phenotype):
        partitioned_heritability(ancestry, phenotype, annots)
        upload_and_remove_files(ancestry, phenotype)


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

    annots = get_all_annot_basenames(ancestry)
    run(ancestry, phenotype, annots)


if __name__ == '__main__':
    main()
