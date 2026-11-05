#!/usr/bin/python3
import argparse
import glob
import json
import math
import numpy as np
import os
import re
from scipy.stats import norm
import shutil
import subprocess

downloaded_files = '/mnt/var/ldsc'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_data(phenotype):
    path_in = f'{s3_in}/out/ldsc/staging/heritability/{phenotype}'
    subprocess.check_call(f'aws s3 cp {path_in}/ data/ --recursive', shell=True)


def z_to_p(z):
    return 2 * norm.cdf(-abs(z))


def translate(file):
    with open(file, 'r') as f:
        lines = f.read()
        maybe_h2 = re.findall(r'Total Observed scale h2: (-?[0-9.]*) \(([0-9.]*)\)', lines)
        if len(maybe_h2) > 0:
            h2, se = map(float, maybe_h2[0])
            p = z_to_p(h2 / se)
            return {'h2': h2, 'stdErr': se, 'pValue': p if p > 0 else np.nextafter(0, 1)}


def get_data(phenotype):
    out = {}
    for file in glob.glob(f'data/*/*.log'):
        ancestry = re.findall(r'data/ancestry=([^/]*)/[^/]*.log', file)[0]
        maybe_out = translate(file)
        if maybe_out is not None:
            out[ancestry] = maybe_out
    return out


def fold_data(data, other_data):
    w_data = 1.0 / data['stdErr'] / data['stdErr']
    w_other = 1.0 / other_data['stdErr'] / other_data['stdErr']
    se = math.sqrt(1.0 / (w_data + w_other))
    h2 = (data['h2'] * w_data + other_data['h2'] * w_other) / (w_data + w_other)
    p = z_to_p(h2 / se)
    return {'h2': h2, 'stdErr': se, 'pValue': p if p > 0 else np.nextafter(0, 1)}


def meta_analyze(data):
    data_to_meta_analyze = [output_data for ancestry, output_data in data.items() if ancestry != 'Mixed']
    if len(data_to_meta_analyze) > 0:
        maybe_meta_data = data_to_meta_analyze[0]
        for other_non_mixed_data in data_to_meta_analyze[1:]:
            maybe_meta_data = fold_data(maybe_meta_data, other_non_mixed_data)
        if 'Mixed' not in data or maybe_meta_data['stdErr'] < data['Mixed']['stdErr']:
            data['Mixed'] = maybe_meta_data
    return data


def upload_data(phenotype, data):
    file = f'./{phenotype}.json'
    with open(file, 'w') as f:
        for ancestry, output_data in data.items():
            output_data['phenotype'] = phenotype
            output_data['ancestry'] = ancestry
            f.write(json.dumps(output_data) + '\n')
    subprocess.check_call(['aws', 's3', 'cp', file, f'{s3_out}/out/ldsc/heritability/{phenotype}/'])
    os.remove(file)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', type=str, required=True, help="Phenotype to translate (e.g. T2D)")
    args = parser.parse_args()

    phenotype = args.phenotype

    download_data(phenotype)
    data = get_data(phenotype)
    data = meta_analyze(data)
    upload_data(phenotype, data)
    shutil.rmtree('data')


if __name__ == '__main__':
    main()
