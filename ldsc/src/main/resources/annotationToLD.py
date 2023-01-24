#!/usr/bin/python3
import argparse
import glob
import shutil
import os
import subprocess


downloaded_files = '/mnt/var/ldsc'
ldsc_files = f'{downloaded_files}/ldsc'
g1000_files = f'{downloaded_files}/g1000'
snp_files = f'{downloaded_files}/snps'

s3_in = 's3://dig-analysis-data'
s3_out = 's3://dig-analysis-data'


def get_annot_file(sub_region, ancestry, annotation, CHR):
    file = f'{s3_in}/out/ldsc/regions/{sub_region}/annot/{annotation}/ancestry={ancestry}/{annotation}.{CHR}.annot.gz'
    subprocess.check_call(['aws', 's3', 'cp', file, f'./{ancestry}/{annotation}/'])


def make_annot_ld(ancestry, annotation, CHR):
    print(f'Making ld annot for ancestry: {ancestry}, annotation: {annotation}, chromosome: {CHR}')
    subprocess.check_call([
        'python3', f'{ldsc_files}/ldsc.py',
        '--bfile', f'{g1000_files}/{ancestry}/chr{CHR}',
        '--ld-wind-cm', '1',
        '--annot', f'./{ancestry}/{annotation}/{annotation}.{CHR}.annot.gz',
        '--thin-annot',
        '--out', f'./{ancestry}/{annotation}/{annotation}.{CHR}',
        '--print-snps', f'{snp_files}/hm.{CHR}.snp'
    ])


def upload_and_remove_files(sub_region, ancestry, annotation):
    s3_dir = f'{s3_out}/out/ldsc/regions/{sub_region}/ld_score/{annotation}/ancestry={ancestry}/'
    for file in glob.glob(f'./{ancestry}/{annotation}/*'):
        subprocess.check_call(['aws', 's3', 'cp', file, s3_dir])
    shutil.rmtree(f'./{ancestry}/{annotation}')


def run(sub_region, ancestry, annotation):
    for CHR in range(1, 23):
        get_annot_file(sub_region, ancestry, annotation, CHR)
        make_annot_ld(ancestry, annotation, CHR)
    upload_and_remove_files(sub_region, ancestry, annotation)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sub-region', default='default', type=str,
                        help="Sub region name (default = default)")
    parser.add_argument('--annotation-path', default=None, required=True, type=str,
                        help="<annotation>/<ancestry>")
    args = parser.parse_args()
    sub_region = args.sub_region
    annotation, ancestry = args.annotation_path.split('/')
    run(sub_region, ancestry, annotation)


if __name__ == '__main__':
    main()
