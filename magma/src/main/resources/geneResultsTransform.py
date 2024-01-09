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

files_location = '/mnt/var/magma'
s3_bucket = 'dig-analysis-igvf'
output_s3 = f's3://{s3_bucket}/out/magma/gene-associations'


def get_gene_map():
    out = {}
    with open(f'{files_location}/NCBI37.3.gene.loc', 'r') as f:
        line = f.readline()
        while len(line) > 0:
            split_line = [col.strip() for col in line.split('\t')]
            geneId = int(split_line[0])
            gene = split_line[5]
            out[geneId] = gene
            line = f.readline()
    return out


def download_ancestry_gene_associations(phenotype):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(s3_bucket)
    for file in my_bucket.objects.filter(Prefix=f'out/magma/staging/genes/{phenotype}/').all():
        if re.fullmatch(f'.*/associations\.genes\.out$', file.key):
            ancestry = re.findall(f'.*/ancestry=(\w+)/associations\.genes\.out$', file.key)[0]
            subprocess.check_call([
                'aws', 's3', 'cp', f's3://{s3_bucket}/{file.key}', f'./{phenotype}/{ancestry}/'
            ])


def get_ancestry_partitioned_data(ncbi, phenotype):
    out = {}
    for file in glob.glob(f'./{phenotype}/*/associations.genes.out'):
        ancestry = re.findall(f'.*/(\w+)/associations\.genes\.out$', file)[0]
        out[ancestry] = get_ancestry_specific_data(ncbi, file)
    return out


def get_ancestry_specific_data(ncbi, srcfile):
    out = {}
    with open(srcfile, 'r') as f:
        _ = f.readline()  # unused header
        line = f.readline().strip()
        while len(line) > 0:
            split_line = [col.strip() for col in re.sub(' +', ' ', line).split(' ')]
            geneId = int(split_line[0])
            if geneId in ncbi:
                out[geneId] = {
                    'gene': ncbi[geneId],
                    'nParam': int(split_line[5]),
                    'subjects': int(split_line[6]),
                    'zStat': float(split_line[7]),
                    'pValue': float(split_line[8]) if float(split_line[8]) > 0 else np.nextafter(0, 1)
                }
            line = f.readline().strip()
    return out


def fold_data(data, other_data):
    meta = {}
    for geneId in data.keys() | other_data.keys():
        if geneId in data and geneId in other_data:
            gene_data = data[geneId]
            other_gene_data = other_data[geneId]
            w_data = math.sqrt(gene_data['subjects'])
            w_other = math.sqrt(other_gene_data['subjects'])

            gene = gene_data['gene']
            subjects = gene_data['subjects'] + other_gene_data['subjects']
            z = (gene_data['zStat'] * w_data + other_gene_data['zStat'] * w_other) / math.sqrt(subjects)
            p = norm.cdf(-z)
        else:
            gene_data = data[geneId] if geneId in data else other_data[geneId]

            gene = gene_data['gene']
            subjects = gene_data['subjects']
            z = gene_data['zStat']
            p = gene_data['pValue']

        meta[geneId] = {
            'gene': gene,
            'nParam': None,
            'subjects': subjects,
            'zStat': z,
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
            for geneId in data['Mixed'].keys() | meta_data.keys():
                mixed_subjects = data['Mixed'].get(geneId, {}).get('subjects', 0)
                meta_subjects = meta_data.get(geneId, {}).get('subjects', 0)
                if mixed_subjects < meta_subjects:
                    data['Mixed'][geneId] = meta_data[geneId]
    return data


def make_part_file(phenotype, ancestry, ancestry_data):
    with open(f'./{phenotype}/{phenotype}_{ancestry}.json', 'w') as f:
        for geneId, gene_data in ancestry_data.items():
            gene_data['phenotype'] = phenotype
            gene_data['ancestry'] = ancestry
            f.write(f'{json.dumps(gene_data)}\n')


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
    download_ancestry_gene_associations(args.phenotype)
    print("Downloaded files")

    # read the NCBI data
    ncbi = get_gene_map()
    print("Generated gene map")

    # get all of the ancestry data
    data = get_ancestry_partitioned_data(ncbi, args.phenotype)
    print("Translated ancestry data")
    for ancestry, ancestry_data in data.items():
        print(ancestry, len(ancestry_data))

    # Meta-analyze ancestry-specific data and then replace gene data when Mixed has a lower final sample size
    data = meta_analyze_and_merge(data)
    print("Meta analyzed data")

    upload_and_remove_files(args.phenotype, data)
    print("uploaded and removed data")


if __name__ == '__main__':
    main()
