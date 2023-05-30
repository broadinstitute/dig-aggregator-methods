#!/usr/bin/python3
import argparse
import glob
import os
import shutil
import subprocess

downloaded_files = '/mnt/var/ldsc'
ldsc_files = f'{downloaded_files}/ldsc'
g1000_files = f'{downloaded_files}/g1000'
snp_files = f'{downloaded_files}/snps'

s3_in = 's3://dig-analysis-data'
s3_out = 's3://dig-analysis-data'


def get_region_file(sub_region, region_name):
    file = f'{s3_in}/out/ldsc/regions/merged/{sub_region}/{region_name}/{region_name}.csv'
    subprocess.check_call(['aws', 's3', 'cp', file, f'./{sub_region}/{region_name}/'])


def convert_to_bed(sub_region, region_name):
    with open(f'./{sub_region}/{region_name}/{region_name}.bed', 'w') as f_out:
        with open(f'./{sub_region}/{region_name}/{region_name}.csv', 'r') as f_in:
            line = f_in.readline()
            while len(line) > 0:
                filtered_line = '\t'.join(line.split('\t')[:3])
                f_out.write(f'chr{filtered_line}\n')
                line = f_in.readline()


def make_annot(sub_region, region_name, ancestry, CHR):
    print(f'Making annot for {region_name}, ancestry: {ancestry}, chromosome: {CHR}')
    subprocess.check_call([
        'python3', f'{ldsc_files}/make_annot.py',
        '--bed-file', f'{sub_region}/{region_name}/{region_name}.bed',
        '--bimfile', f'{g1000_files}/{ancestry}/chr{CHR}.bim',
        '--annot-file', f'{sub_region}/{region_name}/{ancestry}/annot/{region_name}.{CHR}.annot.gz'
    ])


def upload_and_remove_files(sub_region, region_name, ancestry):
    s3_dir = f'{s3_out}/out/ldsc/regions/annot/ancestry={ancestry}/{sub_region}/{region_name}/'
    for file in glob.glob(f'{sub_region}/{region_name}/{ancestry}/annot/*'):
        subprocess.check_call(['aws', 's3', 'cp', file, s3_dir])
    shutil.rmtree(f'{sub_region}/{region_name}/{ancestry}')


def run_ancestry(sub_region, region_name, ancestry):
    os.mkdir(f'./{sub_region}/{region_name}/{ancestry}')
    os.mkdir(f'./{sub_region}/{region_name}/{ancestry}/annot')
    for CHR in range(1, 23):
        make_annot(sub_region, region_name, ancestry, CHR)
    upload_and_remove_files(sub_region, region_name, ancestry)


def run(sub_region, ancestries, region_name):
    get_region_file(sub_region, region_name)
    convert_to_bed(sub_region, region_name)
    for ancestry in ancestries:
        run_ancestry(sub_region, region_name, ancestry)
    shutil.rmtree(f'./{sub_region}/{region_name}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sub-region', default=None, required=True, type=str,
                        help="Sub region name (e.g. annotation-tissue)")
    parser.add_argument('--region-name', default=None, required=True, type=str,
                        help="Merge region name.")
    parser.add_argument('--ancestries', default=None, required=True, type=str,
                        help="All g1000 ancestries (e.g. EUR) to run.")
    args = parser.parse_args()
    sub_region = args.sub_region
    region_name = args.region_name
    ancestries = args.ancestries.split(',')
    run(sub_region, ancestries, region_name)


if __name__ == '__main__':
    main()
