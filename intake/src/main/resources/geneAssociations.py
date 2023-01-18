#!/usr/bin/python3

import argparse
import gzip
import json
import os
import re
import subprocess

import numpy as np
import scipy.stats


convert_mask = {
    'hclof_noflag_canonical': 'LoF_HC',
    'hclof_noflag_missense0.8_canonical': 'hclof_noflag_missense0.8_canonical',
    'missense0.5_canonical': 'missense0.5_canonical'
}

base_url = 's3://dig-analysis-data/gene_associations_raw/600k_600traits'
def download_phecode_files(ancestry, phecode):
    all_file = f'./{phecode}.all.formatted.tsv.gz'
    cauchy_file = f'./{phecode}.cauchy.formatted.tsv.gz'
    subprocess.check_call(['aws', 's3', 'cp', f'{base_url}/{ancestry}/all/{phecode}.formatted.tsv.gz', all_file])
    subprocess.check_call(['aws', 's3', 'cp', f'{base_url}/{ancestry}/cauchy/{phecode}.formatted.tsv.gz', cauchy_file])
    return all_file, cauchy_file


pattern = re.compile('[\W]+')
def convert_phenotype(raw_phenotype):
    return pattern.sub("", raw_phenotype)


expected_95th_percentile = scipy.stats.chi2.ppf(0.95, 1)
def to_stat(value):
    n = scipy.stats.norm.ppf(1 - value['pValue'] / 2)
    return n * n

def get_lambda(output):
    return np.percentile(list(map(to_stat, output.values())), 95) / expected_95th_percentile


def get_converted_phenotype_cauchy(ancestry, cauchy_file):
    cauchy_output = {}
    with gzip.open(cauchy_file, 'r') as f:
        header = f.readline().decode()
        line = f.readline().decode()
        while len(line) > 0:
            line_dict = dict(zip(header.split('\t'), line.split('\t')))
            if line_dict['most_sig_beta'] != 'nan':
                cauchy_output[line_dict['gene']] = {
                    'dataset': '600k_600traits',
                    'ancestry': ancestry,
                    'phenotypeMeaning': convert_phenotype(line_dict['Phecode_Meaning']),
                    'phenotype': line_dict['Phecode'],
                    'phenotypeCategory': line_dict['Phecode_Category'],
                    'ensemblId': line_dict['Gene_stable_ID'],
                    'gene': line_dict['gene'],
                    'pValue': float(line_dict['P_cauchy']),
                    'beta': float(line_dict['most_sig_beta']),
                    'masks': []
                }
            line = f.readline().decode()
    return cauchy_output


def get_full_phenotype_output(all_file, cauchy_output):
    with gzip.open(all_file, 'r') as f:
        header = f.readline().decode()
        line = f.readline().decode()
        while len(line) > 0:
            line_dict = dict(zip(header.split('\t'), line.split('\t')))
            mask = {
                'mask': convert_mask[line_dict['mask']],
                'n': float(line_dict['effective_sample_size']),
                'pValue': float(line_dict['pValue']),
                'beta': float(line_dict['beta']),
                'combinedAF': float(line_dict['combinedAF']),
                'stdErr': float(line_dict['stdErr']) if line_dict['stdErr'] != 'NA' else None
            }
            cauchy_output[line_dict['gene']]['masks'].append(mask)
            line = f.readline().decode()
    return cauchy_output


def get_output_with_lambda(output):
    lambda_value = get_lambda(output)
    for key in output:
        output[key]['lambda'] = lambda_value
    return output


def upload_output(ancestry, phecode, output):
    os.mkdir(phecode)
    with open(f'{phecode}/part-00000.json', 'w') as f:
        for line in output.values():
            f.write(f'{json.dumps(line)}\n')
    with open(f'{phecode}/metadata', 'w') as f:
        f.write(f'{{"name": "600k_600traits", "ancestry": "{ancestry}", "phenotype": "{phecode}"}}\n')
    outdir = f's3://dig-analysis-data/gene_associations/600k_600traits/{ancestry}/{phecode}'
    subprocess.check_call(['aws', 's3', 'cp', f'{phecode}/part-00000.json', f'{outdir}/part-00000.json'])
    subprocess.check_call(['aws', 's3', 'cp', f'{phecode}/metadata', f'{outdir}/metadata'])
    os.remove(f'{phecode}/part-00000.json')
    os.remove(f'{phecode}/metadata')
    os.rmdir(phecode)


def main():
    """
    Arguments:  filename
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('--filename', type=str, required=True)

    # parse command line
    args = opts.parse_args()
    phecode = args.filename.split('.formatted.')[0]
    for ancestry in ['Mixed', 'EU']:
        all_file, cauchy_file = download_phecode_files(ancestry, phecode)
        cauchy_output = get_converted_phenotype_cauchy(ancestry, cauchy_file)
        phenotype_output = get_full_phenotype_output(all_file, cauchy_output)
        output_with_lambda = get_output_with_lambda(phenotype_output)
        upload_output(ancestry, phecode, output_with_lambda)
        os.remove(all_file)
        os.remove(cauchy_file)


if __name__ == '__main__':
    main()
