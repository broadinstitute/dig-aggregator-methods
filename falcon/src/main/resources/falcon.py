#!/usr/bin/python3
import argparse
import json
import os
import shutil
from scipy.stats import norm
import subprocess

falcon_path = '/mnt/var/falcon'
pigean_path = '/mnt/var/pigean'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_sumstats(trait_type, trait_group, phenotype):
    prefix = f'{s3_in}/out/falcon/inputs/{trait_type}/{trait_group}/{phenotype}/'
    cmd = ['aws', 's3', 'cp', prefix, 'inputs/sumstats/', '--recursive']
    subprocess.check_call(cmd)


def run_falcon(phenotype):
    custom_env = {
        **os.environ,
        'MODELS_YAML': f'{pigean_path}/aws_pigean_models_s3.falcon.yaml',
        'FALCON_PYTHON_LIB': '/usr/local/lib64/python3.11/site-packages',
        'GENE_FOLDER': f'{falcon_path}/ref/genes/',
        'LD_FOLDER': f'{falcon_path}/ref/LD/',
        'S2G_FOLDER': f'{falcon_path}/ref/V2G/',
        'ANNOTATIONS': f'{falcon_path}/ref/annotations/atac',
        'GENESET_DIR': f'{pigean_path}/',
        f'GENE_MAP': f'{pigean_path}/portal_gencode.gene.map',
        'GENE_LOC': f'{pigean_path}/NCBI37.3.plink.gene.loc',
        'GENE_LOC_HUGE': f'{pigean_path}/NCBI37.3.plink.gene.exons.loc',
        'PIGEAN_PROFILE': f'{pigean_path}/gwas.default.json',
        'PIGEAN_NO_TRACK_FILTERED': '1'
    }
    cmd = [
        f'{falcon_path}/falcon_src/scripts/falcon_pigean/falcon_pigean.sh',
        '--trait', phenotype,
        '--pigean-src', f'{pigean_path}/pigean/src',
        '--falcon-python', '/usr/bin/python3.11',
        '--pigean-python', '/usr/bin/python3.11',
        '--coeff', f'{falcon_path}/ref/coeff/dummy.coeff.tsv',
        '--sumstats-dir', 'inputs/sumstats',
        '--out-dir', 'outputs',
        '--to-step', '9'
    ]
    try:
        subprocess.check_call(cmd, env=custom_env)
    except Exception as e:
        print(e)


def upload(trait_group, phenotype):
    os.makedirs(f'outputs/{phenotype}', exist_ok=True)
    path = f'{s3_out}/out/falcon/staging/falcon/{trait_group}/{phenotype}'
    cmd = ['aws', 's3', 'cp', f'outputs/{phenotype}/', path, '--recursive']
    subprocess.check_call(cmd)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--trait-type', default=None, required=True, type=str,
                        help="sumstats or gene_lists")
    parser.add_argument('--trait-group', default=None, required=True, type=str,
                        help="Trait group")
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    args = parser.parse_args()

    download_sumstats(args.trait_type, args.trait_group, args.phenotype)
    run_falcon(args.phenotype)
    upload(args.trait_group, args.phenotype)

    shutil.rmtree('inputs')
    shutil.rmtree('outputs')


if __name__ == '__main__':
    main()
