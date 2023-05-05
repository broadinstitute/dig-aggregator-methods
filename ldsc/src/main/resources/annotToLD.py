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

CPUs = 16  # For multithreaded combining and splitting annot file


def get_all_annot_file(sub_region, ancestry):
    file = f'{s3_in}/out/ldsc/regions/{sub_region}/annot/ancestry={ancestry}/'
    subprocess.check_call(['aws', 's3', 'cp', file, f'./{ancestry}/', '--recursive'])
    for file in glob.glob(f'./{ancestry}/*/_SUCCESS'):
        os.remove(file)


def get_region_names(ancestry):
    chr_1_files = f'./{ancestry}/*/*.1.annot.gz'
    region_pattern = f'.*/([^/]*)\\.1\\.annot\\.gz'
    return [re.findall(region_pattern, file)[0] for file in glob.glob(chr_1_files)]


def combine_chr_annot(params):
    region_names, ancestry, CHR = params
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


def combine_all_annot(ancestry):
    region_names = get_region_names(ancestry)
    with Pool(CPUs) as p:
        p.map(combine_chr_annot, [(region_names, ancestry, CHR) for CHR in range(1, 23)])


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
    i, ancestry, CHR = args
    print(f'splitting file {i} for CHR {CHR} and ancestry {ancestry}')
    with gzip.open(f'./{ancestry}.combined.{CHR}.l2.ldscore.gz', 'r') as f_ld:
        region_names = [header[:-2] for header in f_ld.readline().decode().strip().split('\t')[3:]]
        files = [f'./{ancestry}/{region_name}/{region_name}.{CHR}.l2.ldscore.gz' for region_name in region_names]
        with gzip.open(files[i], 'w') as f:
            f.write(b'CHR\tSNP\tBP\tL2\n')
            lines = f_ld.readline().strip().split(b'\t')
            while len(lines[0]) > 0:
                f.write(lines[0] + b'\t' + lines[1] + b'\t' + lines[2] + b'\t' + lines[i + 3] + b'\n')
                lines = f_ld.readline().strip().split(b'\t')
            f.close()


def split_ld(ancestry, CHR):
    with gzip.open(f'./{ancestry}.combined.{CHR}.l2.ldscore.gz', 'r') as f_ld:
        region_names = [header[:-2] for header in f_ld.readline().decode().strip().split('\t')[3:]]

    print(f'Splitting ld file for ancestry {ancestry} and chromosome {CHR}')
    with Pool(CPUs) as p:
        p.map(split_ld_file, [(i, ancestry, CHR) for i in range(len(region_names))])

    os.remove(f'./{ancestry}.combined.{CHR}.l2.ldscore.gz')
    split_m(ancestry, CHR, region_names)


def split_m(ancestry, CHR, region_names):
    with open(f'./{ancestry}.combined.{CHR}.l2.M', 'r') as f_M:
        M_values = f_M.readline().strip().split('\t')
    with open(f'./{ancestry}.combined.{CHR}.l2.M_5_50', 'r') as f_M_50:
        M_50_values = f_M_50.readline().strip().split('\t')
    for i, region_name in enumerate(region_names):
        with open(f'./{ancestry}/{region_name}/{region_name}.{CHR}.l2.M', 'w') as f:
            f.write(M_values[i] + '\n')
        with open(f'./{ancestry}/{region_name}/{region_name}.{CHR}.l2.M_5_50', 'w') as f:
            f.write(M_50_values[i] + '\n')
    os.remove(f'./{ancestry}.combined.{CHR}.l2.M')
    os.remove(f'./{ancestry}.combined.{CHR}.l2.M_5_50')


def make_ancestry_ld(ancestry):
    for CHR in range(1, 23):
        make_ld(ancestry, CHR)
        split_ld(ancestry, CHR)


def upload_and_remove_files(sub_region, ancestry):
    s3_dir = f'{s3_out}/out/ldsc/regions/{sub_region}/ld_score/ancestry={ancestry}/'
    subprocess.check_call(['aws', 's3', 'cp', f'./{ancestry}/', s3_dir, '--recursive'])
    shutil.rmtree(f'./{ancestry}')


def run_ancestry(sub_region, ancestry):
    get_all_annot_file(sub_region, ancestry)
    combine_all_annot(ancestry)
    make_ancestry_ld(ancestry)
    upload_and_remove_files(sub_region, ancestry)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sub-region', default='default', type=str,
                        help="Sub region name (default = default)")
    parser.add_argument('--ancestry', required=True, type=str,
                        help="Ancestry (g1000 style e.g. EUR)")
    args = parser.parse_args()
    sub_region = args.sub_region
    ancestry = args.ancestry
    run_ancestry(sub_region, ancestry)


if __name__ == '__main__':
    main()
