#!/usr/bin/python3
import argparse
import glob
import gzip
import os
import re
import shutil
import subprocess


# g1000 ancestries to be run
ancestries = ['AFR', 'AMR', 'EAS', 'EUR', 'SAS']

downloaded_files = '/mnt/var/ldsc'
ldsc_files = f'{downloaded_files}/ldsc'
g1000_files = f'{downloaded_files}/g1000'
snp_files = f'{downloaded_files}/snps'

s3_in = 's3://dig-analysis-data'
s3_out = 's3://psmadbec-test'


def get_all_region_file(sub_region):
    file = f'{s3_in}/out/ldsc/regions/{sub_region}/merged/'
    subprocess.check_call(['aws', 's3', 'cp', file, f'./data/', '--recursive'])


def convert_all_to_bed():
    for file in glob.glob('./data/*/*.csv'):
        region_name = re.findall('.*/([^/]*).csv', file)[0]
        with open(f'./data/{region_name}/{region_name}.bed', 'w') as f_out:
            with open(f'./data/{region_name}/{region_name}.csv', 'r') as f_in:
                line = f_in.readline()
                while len(line) > 0:
                    filtered_line = '\t'.join(line.split('\t')[:3])
                    f_out.write(f'chr{filtered_line}\n')
                    line = f_in.readline()


def make_all_annot(ancestry, CHR):
    region_names = []
    for file in glob.glob('./data/*/*.csv'):
        region_name = re.findall('.*/([^/]*).csv', file)[0]
        if not os.path.exists(f'./{ancestry}/{region_name}'):
            os.mkdir(f'./{ancestry}/{region_name}')
        print(f'Making annot for {region_name}, ancestry: {ancestry}, chromosome: {CHR}')
        subprocess.check_call([
            'python3', f'{ldsc_files}/make_annot.py',
            '--bed-file', f'data/{region_name}/{region_name}.bed',
            '--bimfile', f'{g1000_files}/{ancestry}/chr{CHR}.bim',
            '--annot-file', f'./{ancestry}/{region_name}/{region_name}.{CHR}.annot.gz'
        ])
        region_names.append(region_name)
    combine_all_annot(region_names, ancestry, CHR)


def combine_all_annot(region_names, ancestry, CHR):
    with gzip.open(f'./{ancestry}.combined.{CHR}.annot.gz', 'w') as f_out:
        files = [gzip.open(f'./{ancestry}/{region_name}/{region_name}.{CHR}.annot.gz', 'r') for region_name in region_names]
        for file in files:
            file.readline()
        f_out.write('\t'.join(region_names).encode() + b'\n')
        lines = [file.readline().strip() for file in files]
        while len(lines[0]) > 0:
            f_out.write(b'\t'.join(lines) + b'\n')
            lines = [file.readline().strip() for file in files]
        for file in files:
            file.close()


def make_ld(ancestry, CHR):
    print(f'Making ld annot for ancestry: {ancestry}, chromosome: {CHR}')
    subprocess.check_call([
        'python3', f'{ldsc_files}/ldsc.py',
        '--bfile', f'{g1000_files}/{ancestry}/chr{CHR}',
        '--ld-wind-cm', '1',
        '--annot', f'./{ancestry}.combined.{CHR}.annot.gz',
        '--thin-annot',
        '--out', f'./{ancestry}.combined.{CHR}',
        '--print-snps', f'{snp_files}/hm.{CHR}.snp'
    ])
    split_ld(ancestry, CHR)


def split_ld(ancestry, CHR):
    print(f'Splitting ld file for ancestry {ancestry} and chromosome {CHR}')
    with gzip.open(f'./{ancestry}.combined.{CHR}.l2.ldscore.gz', 'r') as f_ld:
        region_names = [header[:-2] for header in f_ld.readline().decode().strip().split('\t')[3:]]
        files = [gzip.open(f'./{ancestry}/{region_name}/region_name.{CHR}.l2.ldscore.gz', 'w') for region_name in region_names]
        for file in files:
            file.write(b'ANNOT\n')
        lines = f_ld.readline().strip().split('\t')[3:]
        while len(lines[0]) > 0:
            for i, file in enumerate(files):
                file.write(lines[i] + b'\n')
            lines = f_ld.readline().strip().split('\t')[3:]
        for file in files:
            file.close()
    os.rm(f'./{ancestry}.combined.{CHR}.l2.ldscore.gz')
    split_m(ancestry, CHR, region_names)


def split_m(ancestry, CHR, region_names):
    with open(f'./{ancestry}.combined.{CHR}.l2.M', 'r') as f_M:
        M_values = f_M.readline().strip().split('\t')
    with open(f'./{ancestry}.combined.{CHR}.l2.M_5_50', 'r') as f_M_50:
        M_50_values = f_M_50.readline().strip().split('\t')
    for i, region_name in enumerate(region_names):
        with open(f'./{ancestry}/{region_name}/region_name.{CHR}.l2.M', 'w') as f:
            f.write(M_values[i] + '\n')
        with open(f'./{ancestry}/{region_name}/region_name.{CHR}.l2.M_5_50', 'w') as f:
            f.write(M_50_values[i] + '\n')
    os.rm(f'./{ancestry}.combined.{CHR}.l2.M')
    os.rm(f'./{ancestry}.combined.{CHR}.l2.M_5_50')


def upload_and_remove_files(sub_region, ancestry):
    s3_dir = f'{s3_out}/out/ldsc/regions/{sub_region}/ldscore/'
    subprocess.check_call(['aws', 's3', 'cp', f'./{ancestry}/', s3_dir, '--recursive'])
    shutil.rmtree(f'./{ancestry}')


def run_ancestry(sub_region, ancestry):
    os.mkdir(f'./{ancestry}')
    os.mkdir(f'./{ancestry}/ld_score')
    for CHR in range(1, 23):
        make_all_annot(ancestry, CHR)
        make_ld(ancestry, CHR)
    upload_and_remove_files(sub_region, ancestry)


def run(sub_region):
    get_all_region_file(sub_region)
    convert_all_to_bed()
    for ancestry in ancestries:
        run_ancestry(sub_region, ancestry)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sub-region', default='default', type=str,
                        help="Sub region name (default = default)")
    args = parser.parse_args()
    sub_region = args.sub_region
    run(sub_region)


if __name__ == '__main__':
    main()
