#!/usr/bin/python3
import argparse
import glob
import gzip
from multiprocessing import Pool
import os
import re
import shutil
import subprocess

downloaded_files = '/mnt/var/ldsc'
ldsc_files = f'{downloaded_files}/ldsc'
g1000_files = f'{downloaded_files}/g1000'
snp_files = f'{downloaded_files}/snps'

s3_in = 's3://dig-analysis-data'
s3_out = 's3://dig-analysis-data'

CPUs = 8  # For multithreaded combining and splitting annot file


def get_all_annot_file(ancestry, sub_region, regions):
    for region in regions:
        file = f'{s3_in}/out/ldsc/regions/annot/ancestry={ancestry}/{sub_region}/{region}/'
        subprocess.check_call(['aws', 's3', 'cp', file, f'./{ancestry}/{sub_region}/{region}/', '--recursive', '--exclude=_SUCCESS'])


def combine_chr_annot(params):
    ancestry, sub_region, regions, CHR = params
    with gzip.open(f'./{ancestry}.combined.{CHR}.annot.gz', 'w') as f_out:
        files = [gzip.open(f'./{ancestry}/{sub_region}/{region}/{region}.{CHR}.annot.gz', 'r') for region in regions]
        for file in files:
            file.readline()
        f_out.write('\t'.join(regions).encode() + b'\n')
        lines = [file.readline().strip() for file in files]
        while len(lines[0]) > 0:
            f_out.write(b'\t'.join(lines) + b'\n')
            lines = [file.readline().strip() for file in files]
        for file in files:
            file.close()


def combine_all_annot(ancestry, sub_region, regions):
    with Pool(CPUs) as p:
        p.map(combine_chr_annot, [(ancestry, sub_region, regions, CHR) for CHR in range(1, 23)])


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
    os.remove(f'./{ancestry}.combined.{CHR}.annot.gz')
    os.remove(f'./{ancestry}.combined.{CHR}.log')


def split_ld_file(args):
    i, ancestry, sub_region, regions, CHR = args
    print(f'splitting file {i} for CHR {CHR} and ancestry {ancestry}')
    with gzip.open(f'./{ancestry}.combined.{CHR}.l2.ldscore.gz', 'r') as f_ld:
        f_ld.readline() # Useless header
        files = [f'./{ancestry}/{sub_region}/{region}/{region}.{CHR}.l2.ldscore.gz' for region in regions]
        with gzip.open(files[i], 'w') as f:
            f.write(b'CHR\tSNP\tBP\tL2\n')
            lines = f_ld.readline().strip().split(b'\t')
            while len(lines[0]) > 0:
                f.write(lines[0] + b'\t' + lines[1] + b'\t' + lines[2] + b'\t' + lines[i + 3] + b'\n')
                lines = f_ld.readline().strip().split(b'\t')
            f.close()


def split_ld(ancestry, sub_region, regions, CHR):
    print(f'Splitting ld file for ancestry {ancestry} and chromosome {CHR}')
    with Pool(CPUs) as p:
        p.map(split_ld_file, [(i, ancestry, sub_region, regions, CHR) for i in range(len(regions))])

    os.remove(f'./{ancestry}.combined.{CHR}.l2.ldscore.gz')
    split_m(ancestry, sub_region, regions, CHR)


def split_m(ancestry, sub_region, regions, CHR):
    with open(f'./{ancestry}.combined.{CHR}.l2.M', 'r') as f_M:
        M_values = f_M.readline().strip().split('\t')
    with open(f'./{ancestry}.combined.{CHR}.l2.M_5_50', 'r') as f_M_50:
        M_50_values = f_M_50.readline().strip().split('\t')
    for i, region in enumerate(regions):
        with open(f'./{ancestry}/{sub_region}/{region}/{region}.{CHR}.l2.M', 'w') as f:
            f.write(M_values[i] + '\n')
        with open(f'./{ancestry}/{sub_region}/{region}/{region}.{CHR}.l2.M_5_50', 'w') as f:
            f.write(M_50_values[i] + '\n')
    os.remove(f'./{ancestry}.combined.{CHR}.l2.M')
    os.remove(f'./{ancestry}.combined.{CHR}.l2.M_5_50')


def make_ancestry_ld(ancestry, sub_region, regions):
    for CHR in range(1, 23):
        make_ld(ancestry, CHR)
        split_ld(ancestry, sub_region, regions, CHR)


def upload_and_remove_files(ancestry, sub_region, regions):
    for region in regions:
        s3_dir = f'{s3_out}/out/ldsc/regions/ld_score/ancestry={ancestry}/{sub_region}/{region}/'
        subprocess.check_call(['touch', f'./{ancestry}/{sub_region}/{region}/_SUCCESS'])
        subprocess.check_call(['aws', 's3', 'cp', f'./{ancestry}/{sub_region}/{region}/', s3_dir, '--recursive'])
    shutil.rmtree(f'./{ancestry}')


def run_ancestry(ancestry, sub_region, regions):
    get_all_annot_file(ancestry, sub_region, regions)
    combine_all_annot(ancestry, sub_region, regions)
    make_ancestry_ld(ancestry, sub_region, regions)
    upload_and_remove_files(ancestry, sub_region, regions)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ancestry', required=True, type=str,
                        help="Ancestry (g1000 style e.g. EUR)")
    parser.add_argument('--sub-region', default='default', type=str,
                        help="Sub region name (default = default)")
    parser.add_argument('--regions', required=True, type=str,
                        help="A list of regions in ancestry/sub_region to run")
    args = parser.parse_args()
    ancestry = args.ancestry
    sub_region = args.sub_region
    regions = args.regions.split(',')
    run_ancestry(ancestry, sub_region, regions)


if __name__ == '__main__':
    main()
