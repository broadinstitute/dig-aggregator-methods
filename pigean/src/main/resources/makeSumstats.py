#!/usr/bin/python3
import argparse
import glob
import gzip
import json
from multiprocessing import Pool
import os
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

cpus = 8


def download_data(phenotype):
    path_in = f'{s3_in}/out/metaanalysis/bottom-line/trans-ethnic/{phenotype}/'
    subprocess.check_call(['aws', 's3', 'cp', path_in, 'data/', '--recursive', '--exclude="*_SUCCESS"'])


def convert_single_file(file):
    subprocess.check_call(['zstd', '-d', '--rm', file])
    part_loc, extension, _ = file.split('.')
    file_in = f'{part_loc}.{extension}'
    file_out = f'{part_loc}.sumstats.gz'
    with gzip.open(file_out, 'wt') as f_out:
        with open(file_in, 'r') as f_in:
            for line in f_in:
                json_line = json.loads(line)
                f_out.write('\t'.join([
                    json_line['chromosome'],
                    str(json_line['position']),
                    str(json_line['pValue']),
                    str(json_line['n'])
                ]) + '\n')
    os.remove(file_in)


def combine_and_upload(phenotype):
    file_out = f'{phenotype}.sumstats.gz'
    with gzip.open(file_out, 'w') as f_out:
        f_out.write(b'CHROM\tPOS\tP\tN\n')
    subprocess.run(f'cat data/*.sumstats.gz >> {file_out}', shell=True)
    shutil.rmtree('data')
    subprocess.check_call(['aws', 's3', 'cp', file_out, f'{s3_out}/out/pigean/sumstats/{phenotype}/'])
    os.remove(file_out)


def success(phenotype):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', f'{s3_out}/out/pigean/sumstats/{phenotype}/'])
    os.remove('_SUCCESS')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    args = parser.parse_args()
    download_data(args.phenotype)
    with Pool(cpus) as p:
        p.map(convert_single_file, glob.glob('data/*.json.zst'))
    combine_and_upload(args.phenotype)
    success(args.phenotype)


if __name__ == '__main__':
    main()
