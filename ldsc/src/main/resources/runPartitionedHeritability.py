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

s3_in = 's3://dig-giant-sandbox'
s3_out = 's3://dig-giant-sandbox'


def make_path(split_path):
    for i in range(len(split_path)):
        path = './{}'.format('/'.join(split_path[:(i+1)]))
        if not os.path.exists(path):
            os.mkdir(path)


def get_region(ancestry, sub_region, region):
    g1000_ancestry = ancestry_map[ancestry]
    file = f'{s3_in}/out/ldsc/regions/combined_ld/ancestry={g1000_ancestry}/{sub_region}/{region}/'
    path_out = f'./data/annot/ancestry={g1000_ancestry}/{sub_region}/{region}/'
    subprocess.check_call(['aws', 's3', 'cp', file, path_out, '--recursive', '--exclude=_SUCCESS'])


def get_sumstats(ancestry, phenotype_sexes):
    gathered_phenotypes = []
    for phenotype_sex in phenotype_sexes:
        phenotype, sex = phenotype_sex.split('/')
        file = f'{s3_in}/out/ldsc/sumstats/{phenotype}/ancestry={ancestry}/sex={sex}/{phenotype}_{ancestry}_{sex}.sumstats.gz'
        make_path(['data', 'sumstats', f'{phenotype}', f'ancestry={ancestry}', f'sex={sex}'])
        out = subprocess.run(['aws', 's3', 'cp', file, f'./data/sumstats/{phenotype}/ancestry={ancestry}/sex={sex}/'])
        if out.returncode == 0:
            gathered_phenotypes.append(phenotype_sex.split('/'))
    return gathered_phenotypes


def partitioned_heritability(ancestry, phenotype_sexes, sub_region, region):
    print(f'Partitioned heritability for ancestry: {ancestry} for {len(phenotype_sexes)} phenotypes, '
          f'sub_region: {sub_region}, for {region}')
    g1000_ancestry = ancestry_map[ancestry]
    annotation, tissue = region.split('___')
    annot_str = f'./data/annot/ancestry={g1000_ancestry}/{sub_region}/{region}/{annotation}.{tissue}.'
    phenotype_str = ','.join([
        f'./data/sumstats/{phenotype}/ancestry={ancestry}/sex={sex}/{phenotype}_{ancestry}_{sex}.sumstats.gz' for phenotype, sex in phenotype_sexes
    ])
    phenotype_out_str = ','.join([
        f'./{ancestry}_{phenotype}_{sex}/{ancestry}.{phenotype}.{sex}.{annotation}.{tissue}' for phenotype, sex in phenotype_sexes
    ])
    for phenotype, sex in phenotype_sexes:
        os.mkdir(f'{ancestry}_{phenotype}_{sex}')
    subprocess.check_call([
        'python3', f'{ldsc_files}/ldsc.py',
        '--h2', phenotype_str,
        '--ref-ld-chr', f'{baseline_files}/{g1000_ancestry}/baselineLD.,{annot_str}',
        '--h2-threads', '22',
        '--w-ld-chr', f'{weight_files}/{g1000_ancestry}/weights.',
        '--overlap-annot', '--print-coefficients',
        '--frqfile-chr', f'{frq_files}/{g1000_ancestry}/chr.',
        '--out', phenotype_out_str
    ])


def upload_and_remove_files(ancestry, phenotype_sexes, sub_region, region):
    for phenotype, sex in phenotype_sexes:
        s3_dir = f'{s3_out}/out/ldsc/staging/partitioned_heritability/{phenotype}/ancestry={ancestry}/sex={sex}/{sub_region}/{region}/'
        subprocess.check_call(['touch', f'./{ancestry}_{phenotype}_{sex}/_SUCCESS'])
        subprocess.check_call(['aws', 's3', 'cp', f'./{ancestry}_{phenotype}_{sex}/', s3_dir, '--recursive'])
        shutil.rmtree(f'./{ancestry}_{phenotype}_{sex}')
    shutil.rmtree('./data')


# Need to check on sumstats existence for Mixed ancestry datasets
def run(ancestry, phenotype_sexes, sub_region, region):
    get_region(ancestry, sub_region, region)
    gathered_phenotype_sexes = get_sumstats(ancestry, phenotype_sexes)
    if len(gathered_phenotype_sexes) > 0:
        partitioned_heritability(ancestry, gathered_phenotype_sexes, sub_region, region)
        upload_and_remove_files(ancestry, gathered_phenotype_sexes, sub_region, region)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotypes', type=str, required=True,
                        help="List of phenotype/sex pairs in ancestry to be run (comma separated, slash between phenotype and sex)")
    parser.add_argument('--ancestry', type=str, required=True,
                        help="Short ancestry name (e.g. EU)")
    parser.add_argument('--sub-region', type=str, required=True,
                        help="sub region for list of regions (e.g. annotation-tissue)")
    parser.add_argument('--region', type=str, required=True,
                        help="Region to be run (e.g. accessible_chromatin___pancreas)")
    args = parser.parse_args()

    phenotype_sexes = args.phenotypes.split(',')
    ancestry = args.ancestry
    sub_region = args.sub_region
    region = args.region

    if ancestry not in ancestry_map:
        raise Exception(f'Invalid ancestry ({ancestry}), must be one of {", ".join(ancestry_map.keys())}')

    run(ancestry, phenotype_sexes, sub_region, region)


if __name__ == '__main__':
    main()
