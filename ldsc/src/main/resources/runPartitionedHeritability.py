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

this_project = os.environ['PROJECT']
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def get_sumstats(ancestry, phenotypes):
    gathered_phenotypes = []
    for phenotype in phenotypes:
        file = f'{s3_in}/out/ldsc/sumstats/{phenotype}/ancestry={ancestry}/{phenotype}_{ancestry}.sumstats.gz'
        out = subprocess.run(['aws', 's3', 'cp', file, f'./data/sumstats/{phenotype}/ancestry={ancestry}/'])
        if out.returncode == 0:
            gathered_phenotypes.append(phenotype)
    return gathered_phenotypes


def partitioned_heritability(ancestry, phenotypes, sub_region, regions, project):
    print(f'Partitioned heritability for ancestry: {ancestry} for {len(phenotypes)} phenotypes, '
          f'sub_region: {sub_region}, for {len(regions)}')
    g1000_ancestry = ancestry_map[ancestry]
    annot_strs = []
    for region in regions:
        annotation, tissue = region.split('___')
        annot_strs.append(f'{annot_files}/{project}/ancestry={g1000_ancestry}/{sub_region}/{region}/{annotation}.{tissue}.')
    annot_str = ','.join(annot_strs)
    phenotype_str = ','.join([
        f'./data/sumstats/{phenotype}/ancestry={ancestry}/{phenotype}_{ancestry}.sumstats.gz' for phenotype in phenotypes
    ])
    phenotype_out_str = ','.join([
        f'./{project}/{ancestry}_{phenotype}/{ancestry}.{phenotype}' for phenotype in phenotypes
    ])
    os.mkdir(project)
    for phenotype in phenotypes:
        os.mkdir(f'{project}/{ancestry}_{phenotype}')
    subprocess.check_call([
        'python3', f'{ldsc_files}/ldsc.py',
        '--h2', phenotype_str,
        '--ref-ld-chr', f'{baseline_files}/{g1000_ancestry}/baselineLD.,{annot_str}',
        '--h2-threads', '22', '--xtx-override', '--h2-split-annot',
        '--w-ld-chr', f'{weight_files}/{g1000_ancestry}/weights.',
        '--overlap-annot', '--print-coefficients',
        '--frqfile-chr', f'{frq_files}/{g1000_ancestry}/chr.',
        '--out', phenotype_out_str
    ])


def make_path(split_path):
    for i in range(len(split_path)):
        path = '/'.join(split_path[:(i+1)])
        if not os.path.exists(path):
            os.mkdir(path)


def upload_and_remove_files(ancestry, phenotypes, sub_region, regions, project):
    for phenotype in phenotypes:
        for region in regions:
            annotation, tissue = region.split('___')
            file = f'{ancestry}.{phenotype}.{annotation}.{tissue}.results'
            make_path(['data', 'out', phenotype, f'ancestry={ancestry}', sub_region, region])
            out_path = f'data/out/{phenotype}/ancestry={ancestry}/{sub_region}/{region}'
            os.rename(f'./{project}/{ancestry}_{phenotype}/{file}', f'{out_path}/{file}')
        subprocess.check_call(['touch', f'data/out/{phenotype}/_SUCCESS'])

    s3_dir = f'{s3_out}/out/ldsc/staging/partitioned_heritability/{project}/'
    subprocess.check_call(['aws', 's3', 'cp', 'data/out/', s3_dir, '--recursive'])
    shutil.rmtree(f'./{project}')


# Need to check on sumstats existence for Mixed ancestry datasets
def run(ancestry, phenotypes, sub_region, regions):
    gathered_phenotypes = get_sumstats(ancestry, phenotypes)
    if len(gathered_phenotypes) > 0:
        for project in set(['portal', this_project]):
            partitioned_heritability(ancestry, gathered_phenotypes, sub_region, regions, project)
            upload_and_remove_files(ancestry, gathered_phenotypes, sub_region, regions, project)
    shutil.rmtree('./data')


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
