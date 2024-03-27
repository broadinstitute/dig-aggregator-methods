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

s3_in = 's3://dig-analysis-data'
s3_out = 's3://dig-analysis-data'


def get_regions(ancestry, sub_region, regions):
    g1000_ancestry = ancestry_map[ancestry]
    for region in regions:
        file = f'{s3_in}/out/ldsc/regions/combined_ld/ancestry={g1000_ancestry}/{sub_region}/{region}/'
        path_out = f'./data/annot/ancestry={g1000_ancestry}/{sub_region}/{region}/'
        subprocess.check_call(['aws', 's3', 'cp', file, path_out, '--recursive', '--exclude=_SUCCESS'])


def get_sumstats(ancestry, phenotypes):
    gathered_phenotypes = []
    for phenotype in phenotypes:
        file = f'{s3_in}/out/ldsc/sumstats/{phenotype}/ancestry={ancestry}/{phenotype}_{ancestry}.sumstats.gz'
        out = subprocess.run(['aws', 's3', 'cp', file, f'./data/sumstats/{phenotype}/ancestry={ancestry}/{ancestry}.{phenotype}.sumstats.gz'])
        if out.returncode == 0:
            gathered_phenotypes.append(phenotype)
    return gathered_phenotypes


def get_strings(ancestry, sub_region, regions, phenotypes):
    g1000_ancestry = ancestry_map[ancestry]
    annot_strs = []
    phenotype_strs = []
    for region in regions:
        annotation, tissue = region.split('___')
        annot_strs.append(f'./data/annot/ancestry={g1000_ancestry}/{sub_region}/{region}/{annotation}.{tissue}.')
    for phenotype in phenotypes:
        phenotype_strs.append(f'./data/sumstats/{phenotype}/ancestry={ancestry}/{ancestry}.{phenotype}.sumstats.gz')
    os.mkdir('data/out')
    for region in regions:
        annotation, tissue = region.split('___')
        for phenotype in phenotypes:
            os.mkdir(f'data/out/{ancestry}.{phenotype}.{annotation}.{tissue}')
    return ','.join(annot_strs), ','.join(phenotype_strs)


def partitioned_heritability(ancestry, phenotypes, sub_region, regions):
    print(f'Partitioned heritability for ancestry: {ancestry} for {len(phenotypes)} phenotypes, '
          f'sub_region: {sub_region}, for {len(regions)} regions')
    g1000_ancestry = ancestry_map[ancestry]
    annot_str, phenotype_str = get_strings(ancestry, sub_region, regions, phenotypes)
    subprocess.check_call([
        'python3', f'{ldsc_files}/ldsc.py',
        '--h2', phenotype_str,
        '--ref-ld-chr', f'{baseline_files}/{g1000_ancestry}/baselineLD.,{annot_str}',
        '--h2-threads', '22',
        '--w-ld-chr', f'{weight_files}/{g1000_ancestry}/weights.',
        '--overlap-annot', '--print-coefficients',
        '--frqfile-chr', f'{frq_files}/{g1000_ancestry}/chr.',
        '--out', 'data/out'
    ])


def upload_and_remove_files(ancestry, phenotypes, sub_region, regions):
    for phenotype in phenotypes:
        for region in regions:
            annotation, tissue = region.split('___')
            s3_dir = f'{s3_out}/out/ldsc/staging/partitioned_heritability/{phenotype}/ancestry={ancestry}/{sub_region}/{region}/'
            subprocess.check_call(['touch', f'data/out/{ancestry}.{phenotype}.{annotation}.{tissue}/_SUCCESS'])
            subprocess.check_call(['aws', 's3', 'cp', f'./data/out/{ancestry}.{phenotype}.{annotation}.{tissue}/', s3_dir, '--recursive'])
    shutil.rmtree('./data')


# Need to check on sumstats existence for Mixed ancestry datasets
def run(ancestry, phenotypes, sub_region, regions):
    get_regions(ancestry, sub_region, regions)
    gathered_phenotypes = get_sumstats(ancestry, phenotypes)
    if len(gathered_phenotypes) > 0:
        partitioned_heritability(ancestry, gathered_phenotypes, sub_region, regions)
        upload_and_remove_files(ancestry, gathered_phenotypes, sub_region, regions)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotypes', type=str, required=True,
                        help="List of phenotypes in ancestry to be run (comma separated)")
    parser.add_argument('--ancestry', type=str, required=True,
                        help="Short ancestry name (e.g. EU)")
    parser.add_argument('--sub-region', type=str, required=True,
                        help="sub region for list of regions (e.g. annotation-tissue)")
    parser.add_argument('--regions', type=str, required=True,
                        help="Region to be run (e.g. accessible_chromatin___pancreas)")
    args = parser.parse_args()

    phenotypes = args.phenotypes.split(',')
    ancestry = args.ancestry
    sub_region = args.sub_region
    regions = args.regions.split(',')

    if ancestry not in ancestry_map:
        raise Exception(f'Invalid ancestry ({ancestry}), must be one of {", ".join(ancestry_map.keys())}')

    run(ancestry, phenotypes, sub_region, regions)


if __name__ == '__main__':
    main()
