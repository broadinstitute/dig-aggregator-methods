#!/usr/bin/python3
import argparse
import json
import os
import shutil
from scipy.stats import norm
import subprocess

falcon_path = '/mnt/var/falcon'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

chroms = {str(c) for c in range(1, 23)}


def download_sumstats(phenotype):
    prefix = f's3://dig-analysis-data/out/metaanalysis/bottom-line/trans-ethnic/{phenotype}/'
    cmd = ['aws', 's3', 'cp', prefix, 'inputs/raw/', '--recursive']
    subprocess.check_call(cmd)


def load_snp_map():
    snp_map = {}
    with open(f'{falcon_path}/snp.csv') as f:
        next(f)  # header: dbSNP, varId
        for line in f:
            rsid, var_id = line.rstrip('\n').split('\t')
            snp_map[var_id] = rsid
    return snp_map


def reformat_sumstats(snp_map):
    os.makedirs('inputs/sumstats', exist_ok=True)
    out_files = {}
    count = 0
    for chrom in chroms:
        f = open(f'inputs/sumstats/{chrom}.sumstats', 'w')
        f.write('varId\tCHROM\tPOS\tREF\tALT\tpVALUE\tBETA\tSE\tZ\tN\trsID\n')
        out_files[chrom] = f
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
            if row['pValue'] == 1:
                continue
            if row['pValue'] < 1E-300:
                row['pValue'] = 1E-300
            row['Z'] = abs(norm.ppf(row['pValue'] / 2.0))
            row['Z'] = row['Z'] if row['beta'] > 0 else -row['Z']
            row['stdErr'] = row['beta'] / row['Z']
            if row['stdErr'] == 0 or abs(row['Z']) <= 5:
                continue
            out_files[chrom].write(
                f"{row['varId']}\t{chrom}\t{row['position']}\t{row['reference']}\t{row['alt']}\t"
                f"{row['pValue']}\t{row['beta']}\t{row['stdErr']}\t{row['Z']}\t{row['n']}\t{rsid}\n"
            )
            count += 1
        proc.stdout.close()
        proc.wait()
    for f in out_files.values():
        f.close()
    shutil.rmtree('inputs/raw')
    return count


def upload(phenotype):
    path = f'{s3_out}/out/falcon/inputs/sumstats/portal/{phenotype}/'
    cmd = ['aws', 's3', 'cp', f'inputs/sumstats/', path, '--recursive']
    subprocess.check_call(cmd)
    success(path)


def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Phenotype to process; selects which sumstats to download from S3")
    args = parser.parse_args()

    download_sumstats(args.phenotype)
    count = reformat_sumstats(load_snp_map())
    if count > 0:
        upload(args.phenotype)

    shutil.rmtree('inputs')


if __name__ == '__main__':
    main()
