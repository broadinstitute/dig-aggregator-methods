#!/usr/bin/python3
import argparse
import glob
import os
import shutil
import subprocess
import gzip

downloaded_files = '/mnt/var/ldsc'
g1000_files = f'{downloaded_files}/g1000'
snp_files = f'{downloaded_files}/snps'

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def get_region_file(sub_region, region_name):
    file = f'{s3_in}/out/ldsc/regions/merged/{sub_region}/{region_name}/{region_name}.csv'
    subprocess.check_call(['aws', 's3', 'cp', file, f'./{sub_region}/{region_name}/'])


def parse_range_line(line):
    chromosome, pos_start_str, pos_end_str = line.split('\t')[:3]
    return chromosome, int(pos_start_str), int(pos_end_str)


def get_parsed_range_line(range_f):
    line = range_f.readline().strip()
    if len(line) == 0:
        return None, None, None
    else:
        return parse_range_line(line)


def set_range_start(range_f, g1000_chr):
    range_chr, pos_start, pos_end = get_parsed_range_line(range_f)
    while range_chr is not None and range_chr != g1000_chr:
        range_chr, pos_start, pos_end = get_parsed_range_line(range_f)
    return range_chr, pos_start, pos_end


def parse_g1000_line(line):
    chromosome, _, _, position_str = line.split('\t')[:4]
    return chromosome, int(position_str)


def get_parsed_g1000_line(g1000_f):
    line = g1000_f.readline().strip()
    if len(line) == 0:
        return None, None
    else:
        return parse_g1000_line(line)


def make_annot(sub_region, region_name, ancestry, CHR):
    print(f'Making annot for {region_name}, ancestry: {ancestry}, chromosome: {CHR}')
    with gzip.open(f'{sub_region}/{region_name}/{ancestry}/annot/{region_name}.{CHR}.annot.gz', 'w') as annot_f:
        annot_f.write(b'ANNOT\n')
        with open(f'{g1000_files}/{ancestry}/chr{CHR}.bim', 'r') as g1000_f:
            with open(f'./{sub_region}/{region_name}/{region_name}.csv', 'r') as range_f:
                g1000_chr, position = get_parsed_g1000_line(g1000_f)
                range_chr, pos_start, pos_end = set_range_start(range_f, g1000_chr)
                while range_chr is not None and g1000_chr is not None and g1000_chr == range_chr:
                    if position <= pos_start:
                        annot_f.write(b'0\n')
                        g1000_chr, position = get_parsed_g1000_line(g1000_f)
                    elif position <= pos_end:
                        annot_f.write(b'1\n')
                        g1000_chr, position = get_parsed_g1000_line(g1000_f)
                    else:
                        range_chr, pos_start, pos_end = get_parsed_range_line(range_f)
                # Finish off the g1000 beyond last range
                while g1000_chr is not None:
                    annot_f.write(b'0\n')
                    g1000_chr, position = get_parsed_g1000_line(g1000_f)


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
    for ancestry in ancestries:
        run_ancestry(sub_region, region_name, ancestry)
    shutil.rmtree(f'./{sub_region}/{region_name}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sub-region', default=None, required=True, type=str,
                        help="Merge sub region.")
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
