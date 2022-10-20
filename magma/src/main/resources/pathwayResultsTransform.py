#!/usr/bin/python3
import argparse
import boto3
import glob
import json
import math
import numpy as np
import os
import re
from scipy.stats import norm
import shutil
import subprocess

output_s3 = 's3://dig-analysis-data/out/magma/pathway-associations'


def download_ancestry_pathway_associations(phenotype):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('dig-analysis-data')
    for file in my_bucket.objects.filter(Prefix=f'out/magma/staging/pathways/{phenotype}/').all():
        if re.fullmatch(f'.*/associations\.pathways\.gsa\.out$', file.key):
            ancestry = re.findall(f'.*/ancestry=(\w+)/associations\.pathways\.gsa\.out$', file.key)[0]
            subprocess.check_call([
                'aws', 's3', 'cp', f's3://dig-analysis-data/{file.key}', f'./{phenotype}/{ancestry}/'
            ])


def get_ancestry_partitioned_data(phenotype):
    out = {}
    for file in glob.glob(f'./{phenotype}/*/associations.pathways.gsa.out'):
        ancestry = re.findall(f'.*/(\w+)/associations\.pathways\.gsa\.out$', file)[0]
        out[ancestry] = get_ancestry_specific_data(file)
    return out


def get_ancestry_specific_data(srcfile):
    out = {}
    with open(srcfile, 'r') as f:
        # Remove header
        line = f.readline().strip()
        while line[0] == '#':
            line = f.readline().strip()

        line = f.readline().strip()
        while len(line) > 0:
            split_line = [col.strip() for col in re.sub(' +', ' ', line).split(' ')]
            pathway_name = split_line[7]
            out[pathway_name] = {
                'pathwayName': pathway_name,
                'numGenes': int(split_line[2]),
                'beta': float(split_line[3]),
                'betaStdErr': float(split_line[4]),
                'stdErr': float(split_line[5]),
                'pValue': float(split_line[6]) if float(split_line[6]) > 0 else np.nextafter(0, 1)
            }
            line = f.readline().strip()
    return out


def fold_data(data, other_data):
    meta = {}
    for pathway_name in data.keys() | other_data.keys():
        if pathway_name in data and pathway_name in other_data:
            pathway_data = data[pathway_name]
            other_pathway_data = other_data[pathway_name]
            w_data = 1 / pathway_data['stdErr'] / pathway_data['stdErr']
            w_other = 1 / other_pathway_data['stdErr'] / other_pathway_data['stdErr']
            beta = (pathway_data['beta'] * w_data + other_pathway_data['beta'] * w_other) / (w_data + w_other)
            se = math.sqrt(1.0 / (w_data + w_other))
            p = norm.cdf(-beta / se)
        else:
            pathway_data = data[pathway_name] if pathway_name in data else other_data[pathway_name]
            beta = pathway_data['beta']
            se = pathway_data['stdErr']
            p = pathway_data['pValue']

        meta[pathway_name] = {
            'pathwayName': pathway_name,
            'numGenes': None,
            'beta': beta,
            'betaStdErr': None,  # TODO: Can this be calculated?
            'stdErr': se,
            'pValue': p if p > 0 else np.nextafter(0, 1)
        }
    return meta


def get_mata_analysis(data_to_meta_analyze):
    meta_data = data_to_meta_analyze[0]
    for other_non_mixed_data in data_to_meta_analyze[1:]:
        meta_data = fold_data(meta_data, other_non_mixed_data)
    return meta_data


def meta_analyze_and_merge(data):
    data_to_meta_analyze = [output_data for ancestry, output_data in data.items() if ancestry != 'Mixed']
    if len(data_to_meta_analyze) > 0:
        meta_data = get_mata_analysis(data_to_meta_analyze)
        if 'Mixed' not in data:
            data['Mixed'] = meta_data
        else:
            for pathway_name in data['Mixed'].keys() | meta_data.keys():
                mixed_se = data['Mixed'].get(pathway_name, {}).get('stdErr', math.inf)
                meta_se = meta_data.get(pathway_name, {}).get('stdErr', math.inf)
                if mixed_se > meta_se:
                    data['Mixed'][pathway_name] = meta_data[pathway_name]
    return data


def make_part_file(phenotype, ancestry, ancestry_data):
    with open(f'./{phenotype}/{phenotype}_{ancestry}.json', 'w') as f:
        for pathway_name, pathway_data in ancestry_data.items():
            pathway_data['phenotype'] = phenotype
            pathway_data['ancestry'] = ancestry
            f.write(f'{json.dumps(pathway_data)}\n')


def upload_and_remove_files(phenotype, data):
    for ancestry, ancestry_data in data.items():
        make_part_file(phenotype, ancestry, ancestry_data)
        subprocess.check_call(['aws', 's3', 'cp',
                               f'./{phenotype}/{phenotype}_{ancestry}.json',
                               f'{output_s3}/{phenotype}/ancestry={ancestry}/part-00000.json'])
    if os.path.exists(f'./{phenotype}/'):
        shutil.rmtree(f'./{phenotype}/')


def main():
    """
    Arguments: phenotype
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', default=None, required=True, type=str, help="Input phenotype.")
    args = parser.parse_args()

    # Download all ancestry-sepcific files
    download_ancestry_pathway_associations(args.phenotype)
    print("Downloaded files")

    # get all of the ancestry data
    data = get_ancestry_partitioned_data(args.phenotype)
    print("Translated ancestry data")
    for ancestry, ancestry_data in data.items():
        print(ancestry, len(ancestry_data))

    # Meta-analyze ancestry-specific data and then replace pathway data when Mixed has a lower final sample size
    data = meta_analyze_and_merge(data)
    print("Meta analyzed data")

    upload_and_remove_files(args.phenotype, data)
    print("uploaded and removed data")


if __name__ == '__main__':
    main()
