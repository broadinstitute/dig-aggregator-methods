#!/usr/bin/python3
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
baseline_files = f'{downloaded_files}/baseline'
weight_files = f'{downloaded_files}/weights'
frq_files = f'{downloaded_files}/frq'

s3_in = 's3://dig-analysis-data'
s3_out = 's3://psmadbec-test'


def make_path(split_path):
    for i in range(len(split_path)):
        path = './{}'.format('/'.join(split_path[:(i+1)]))
        if not os.path.exists(path):
            os.mkdir(path)


def get_annot_ld(sub_region, ancestry, annot):
    g1000_ancestry = ancestry_map[ancestry]
    file = f'{s3_in}/out/ldsc/regions/{sub_region}/ld_score/ancestry={g1000_ancestry}/{annot}/'
    make_path(['data', 'ld_score', f'ancestry={ancestry}', f'{annot}'])
    subprocess.check_call(['aws', 's3', 'cp', file, f'./data/ld_score/ancestry={ancestry}/{annot}/', '--recursive'])


def get_sumstats(ancestry, phenotype):
    file = f'{s3_in}/out/ldsc/sumstats/{phenotype}/ancestry={ancestry}/{phenotype}_{ancestry}.sumstats.gz'
    make_path(['data', 'sumstats', f'{phenotype}', f'ancestry={ancestry}'])
    subprocess.check_call(['aws', 's3', 'cp', file, f'./data/sumstats/{phenotype}/ancestry={ancestry}/'])


def partitioned_heritability(ancestry, phenotype, annot):
    print(f'Partitioned heritability for ancestry: {ancestry}, phenotype: {phenotype}, annot: {annot}')
    g1000_ancestry = ancestry_map[ancestry]
    subprocess.check_call([
        'python3', f'{ldsc_files}/ldsc.py',
        '--h2', f'./data/sumstats/{phenotype}/ancestry={ancestry}/{phenotype}.sumstats.gz',
        '--ref-ld-chr', f'{baseline_files}/{g1000_ancestry}/baselineLD.,./data/ld_score/ancestry={ancestry}/{annot}/{annot}.',
        '--w-ld-chr', f'{weight_files}/{g1000_ancestry}/weights.',
        '--overlap-annot',
        '--frqfile-chr', f'{frq_files}/{g1000_ancestry}/chr.',
        '--out', f'./{ancestry}.{phenotype}.{annot}'
    ])


# def upload_and_remove_files(sub_region, ancestry):
#     s3_dir = f'{s3_out}/out/ldsc/regions/{sub_region}/ld_score/ancestry={ancestry}/'
#     subprocess.check_call(['aws', 's3', 'cp', f'./{ancestry}/', s3_dir, '--recursive'])
#     shutil.rmtree(f'./{ancestry}')


def run(sub_region, ancestry, phenotype, annot):
    get_annot_ld(sub_region, ancestry, annot)
    get_sumstats(ancestry, phenotype)
    partitioned_heritability(ancestry, phenotype, annot)
    #upload_and_remove_files(sub_region, ancestry)


def main():
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--sub-region', default='default', type=str,
    #                     help="Sub region name (default = default)")
    # args = parser.parse_args()
    # sub_region = args.sub_region
    sub_region = 'default'
    ancestry = 'EU'
    phenotype = 'BMI'
    annot = 'accessible_chromatin___adipose_tissue'
    run(sub_region, ancestry, phenotype, annot)


if __name__ == '__main__':
    main()
