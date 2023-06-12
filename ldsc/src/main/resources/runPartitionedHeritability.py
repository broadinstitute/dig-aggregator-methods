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
annot_files = f'./data/annot'

s3_in = 's3://dig-analysis-data'
s3_out = 's3://psmadbec-test'


def make_path(split_path):
    for i in range(len(split_path)):
        path = './{}'.format('/'.join(split_path[:(i+1)]))
        if not os.path.exists(path):
            os.mkdir(path)


def get_regions(ancestry, sub_region, regions):
    g1000_ancestry = ancestry_map[ancestry]
    for region in regions:
        file = f'{s3_in}/out/ldsc/regions/ld_score/ancestry={g1000_ancestry}/{sub_region}/{region}/'
        make_path(['data', 'annot', f'{sub_region}', f'{region}'])
        subprocess.check_call(['aws', 's3', 'cp',
                               file, f'./data/annot/ancestry={g1000_ancestry}/{sub_region}/{region}/',
                               '--recursive', '--exclude=_SUCCESS'
                               ])


def get_sumstats(ancestry, phenotypes):
    gathered_phenotypes = []
    for phenotype in phenotypes:
        file = f'{s3_in}/out/ldsc/sumstats/{phenotype}/ancestry={ancestry}/{phenotype}_{ancestry}.sumstats.gz'
        make_path(['data', 'sumstats', f'{phenotype}', f'ancestry={ancestry}'])
        out = subprocess.run(['aws', 's3', 'cp', file, f'./data/sumstats/{phenotype}/ancestry={ancestry}/'])
        if out.returncode == 0:
            gathered_phenotypes.append(phenotype)
    return gathered_phenotypes


def partitioned_heritability(ancestry, phenotypes, sub_region, regions):
    print(f'Partitioned heritability for ancestry: {ancestry} for {len(phenotypes)} phenotypes, '
          f'sub_region: {sub_region}, for {len(regions)}')
    g1000_ancestry = ancestry_map[ancestry]
    annot_str = ','.join([
        f'{annot_files}/ancestry={g1000_ancestry}/{sub_region}/{region}/{region}.' for region in regions
    ])
    phenotype_str = ','.join([
        f'./data/sumstats/{phenotype}/ancestry={ancestry}/{phenotype}_{ancestry}.sumstats.gz' for phenotype in phenotypes
    ])
    phenotype_out_str = ','.join([
        f'./{ancestry}_{phenotype}/{ancestry}.{phenotype}' for phenotype in phenotypes
    ])
    for phenotype in phenotypes:
        os.mkdir(f'{ancestry}_{phenotype}')
    subprocess.check_call([
        'python3', f'{ldsc_files}/ldsc.py',
        '--h2', phenotype_str,
        '--ref-ld-chr', f'{baseline_files}/{g1000_ancestry}/baselineLD.,{annot_str}',
        '--h2-split-annot', '--h2-threads', '22', '--xtx-override',
        '--w-ld-chr', f'{weight_files}/{g1000_ancestry}/weights.',
        '--overlap-annot', '--print-coefficients',
        '--frqfile-chr', f'{frq_files}/{g1000_ancestry}/chr.',
        '--out', phenotype_out_str
    ])


def upload_and_remove_files(ancestry, phenotypes):
    for phenotype in phenotypes:
        s3_dir = f'{s3_out}/out/ldsc/staging/partitioned_heritability/{phenotype}/ancestry={ancestry}/'
        subprocess.check_call(['touch', f'./{ancestry}_{phenotype}/_SUCCESS'])
        subprocess.check_call(['aws', 's3', 'cp', f'./{ancestry}_{phenotype}/', s3_dir, '--recursive'])
        shutil.rmtree(f'./{ancestry}_{phenotype}')
    shutil.rmtree('./data')


# Need to check on sumstats existence for Mixed ancestry datasets
def run(ancestry, phenotypes, sub_region, regions):
    gathered_phenotypes = get_sumstats(ancestry, phenotypes)
    if len(gathered_phenotypes) > 0:
        partitioned_heritability(ancestry, gathered_phenotypes, sub_region, regions)
        upload_and_remove_files(ancestry, gathered_phenotypes)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotypes', type=str, required=True,
                        help="List of phenotypes in ancestry to be run (comma separated)")
    parser.add_argument('--ancestry', type=str, required=True,
                        help="Short ancestry name (e.g. EU)")
    parser.add_argument('--sub-region', type=str, required=True,
                        help="sub region for list of regions (e.g. annotation-tissue)")
    parser.add_argument('--regions', type=str, required=True,
                        help="List of regions to be run (comma separated)")
    args = parser.parse_args()

    phenotypes = args.phenotype.split(',')
    ancestry = args.ancestry
    sub_region = args.sub_region
    regions = args.regions.split(',')

    if ancestry not in ancestry_map:
        raise Exception(f'Invalid ancestry ({ancestry}), must be one of {", ".join(ancestry_map.keys())}')

    run(ancestry, phenotypes, sub_region, regions)


if __name__ == '__main__':
    main()
