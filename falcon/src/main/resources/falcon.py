#!/usr/bin/python3
import argparse
import json
import os
import shutil
import subprocess

binary_path = '/mnt/var/falcon/falcon'
config_file = '/mnt/var/falcon/ref/falcon.ini'
snp_map_file = '/mnt/var/falcon/ref/snp.csv'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

chroms = {str(c) for c in range(1, 23)}


def download_sumstats(phenotype):
    prefix = f'{s3_in}/out/metaanalysis/bottom-line/ancestry-specific/{phenotype}/ancestry=EU/'
    cmd = ['aws', 's3', 'cp', prefix, 'inputs/raw/', '--recursive']
    subprocess.check_call(cmd)


def load_snp_map():
    snp_map = {}
    with open(snp_map_file) as f:
        next(f)  # header: dbSNP, varId
        for line in f:
            rsid, var_id = line.rstrip('\n').split('\t')
            snp_map[var_id] = rsid
    return snp_map


def reformat_sumstats(snp_map):
    os.makedirs('inputs/sumstats', exist_ok=True)
    out_files = {}
    for fname in sorted(os.listdir('inputs/raw')):
        if not fname.endswith('.json.zst'):
            continue
        proc = subprocess.Popen(['zstd', '-d', '-c', f'inputs/raw/{fname}'], stdout=subprocess.PIPE, text=True)
        for line in proc.stdout:
            row = json.loads(line)
            chrom = row['chromosome']
            if chrom not in chroms:
                continue
            rsid = snp_map.get(row['varId'])
            if rsid is None:
                continue
            if abs(row['beta'] / row['stdErr']) <= 5:
                continue
            if chrom not in out_files:
                f = open(f'inputs/sumstats/{chrom}.sumstats', 'w')
                f.write('varId\tCHROM\tPOS\tREF\tALT\tBETA\tSE\tN\trsID\n')
                out_files[chrom] = f
            out_files[chrom].write(
                f"{row['varId']}\t{chrom}\t{row['position']}\t{row['reference']}\t{row['alt']}\t"
                f"{row['beta']}\t{row['stdErr']}\t{row['n']}\t{rsid}\n"
            )
        proc.stdout.close()
        proc.wait()
    for f in out_files.values():
        f.close()
    shutil.rmtree('inputs/raw')


def run_falcon(chrom):
    cmd = [
        binary_path,
        '--config-file', config_file,
        '--chr-to-update', str(chrom),
        '--sumstats-folder', 'inputs/sumstats/',
        '--out-base-name', 'outputs/falcon',
    ]
    subprocess.check_call(cmd)


def upload(phenotype):
    path = f'{s3_out}/out/falcon/staging/falcon/{phenotype}/'
    cmd = ['aws', 's3', 'cp', 'outputs/', path, '--recursive']
    subprocess.check_call(cmd)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Phenotype to process; selects which sumstats to download from S3")
    args = parser.parse_args()

    download_sumstats(args.phenotype)
    reformat_sumstats(load_snp_map())

    for chrom in range(1, 23):
        run_falcon(chrom)

    upload(args.phenotype)

    shutil.rmtree('inputs')
    shutil.rmtree('outputs')


if __name__ == '__main__':
    main()
