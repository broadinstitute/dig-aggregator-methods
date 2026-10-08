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

chroms = {str(c) for c in range(1, 23)}


def download_sumstats(phenotype):
    prefix = f'{s3_in}/out/metaanalysis/bottom-line/trans-ethnic/{phenotype}/'
    cmd = ['aws', 's3', 'cp', prefix, 'inputs/raw/', '--recursive']
    subprocess.check_call(cmd)


def load_snp_map():
    snp_map = {}
    with open(f'{falcon_path}/ref/snp.csv') as f:
        next(f)  # header: dbSNP, varId
        for line in f:
            rsid, var_id = line.rstrip('\n').split('\t')
            snp_map[var_id] = rsid
    return snp_map


def reformat_sumstats(snp_map):
    os.makedirs('inputs/sumstats', exist_ok=True)
    out_files = {}
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
        proc.stdout.close()
        proc.wait()
    for f in out_files.values():
        f.close()
    shutil.rmtree('inputs/raw')


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
        '--out-dir', 'outputs'
    ]
    try:
        subprocess.check_call(cmd, env=custom_env)
    except Exception as e:
        print('ERROR: ' + e)


def upload(phenotype):
    os.makedirs(f'outputs/{phenotype}', exist_ok=True)
    path = f'{s3_out}/out/falcon/staging/falcon/{phenotype}/mouse_msigdb/'
    cmd = ['aws', 's3', 'cp', f'outputs/{phenotype}/', path, '--recursive']
    subprocess.check_call(cmd)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Phenotype to process; selects which sumstats to download from S3")
    args = parser.parse_args()

    download_sumstats(args.phenotype)
    reformat_sumstats(load_snp_map())

    run_falcon(args.phenotype)

    upload(args.phenotype)

    shutil.rmtree('inputs')
    shutil.rmtree('outputs')


if __name__ == '__main__':
    main()
