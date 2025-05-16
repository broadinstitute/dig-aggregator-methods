#!/usr/bin/python3

import argparse
import gzip
import json
import os
import re
import subprocess

import numpy as np
import scipy.stats

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

base_url = f'{s3_in}/gene_associations_raw/600k_600traits'
def download_phecode_files(ancestry, cohort, phecode):
    all_file = f'{phecode}.all_masks_formatted.tsv.gz'
    cauchy_file = f'{phecode}.cauchy_formatted.tsv.gz'
    subprocess.check_call(['aws', 's3', 'cp', f'{base_url}/{ancestry}/{cohort}/all/{all_file}', all_file])
    subprocess.check_call(['aws', 's3', 'cp', f'{base_url}/{ancestry}/{cohort}/cauchy/{cauchy_file}', cauchy_file])
    return all_file, cauchy_file


pattern = re.compile('[\W]+')
def convert_phenotype(raw_phenotype):
    return pattern.sub("", raw_phenotype)


expected_95th_percentile = scipy.stats.chi2.ppf(0.95, 1)
def to_stat(value):
    if value is not None:
        n = scipy.stats.norm.ppf(1 - value / 2)
        return n * n


def get_lambda(output, key):
    stats = list(map(lambda value: to_stat(value[key]), output.values()))
    return np.percentile([v for v in stats if v is not None], 95) / expected_95th_percentile


def optional_float(line_dict, key):
    return float(line_dict[key]) if line_dict[key] not in ['', 'NA'] else None


def get_converted_phenotype_cauchy(ancestry, cohort, cauchy_file):
    cauchy_output = {}
    with gzip.open(cauchy_file, 'r') as f:
        header = f.readline().decode().strip()
        line = f.readline().decode().strip()
        while len(line) > 0:
            line_dict = dict(zip(header.split('\t'), line.split('\t')))
            if line_dict['most_sig_beta'] != 'nan':
                cauchy_output[line_dict['gene']] = {
                    'dataset': '600k_600traits',
                    'ancestry': ancestry,
                    'cohort': cohort,
                    'phenotypeMeaning': convert_phenotype(line_dict['Phecode_Meaning']),
                    'phenotype': line_dict['Phecode'],
                    'phenotypeCategory': line_dict['Phecode_Category'],
                    'ensemblId': line_dict['Gene_stable_ID'],
                    'gene': line_dict['gene'],
                    'pValue_rare': optional_float(line_dict, 'P_cauchy'),
                    'pValue_low_freq': float(line_dict['P_cauchy_v2']),
                    'pValue_best_mask': optional_float(line_dict, 'pValue'),
                    'beta': optional_float(line_dict, 'most_sig_beta'),
                    'best_mask': line_dict.get('most_sig_mask_name'),
                    'cases': optional_float(line_dict, 'n.cases_Meta'),
                    'controls': float(line_dict['n.controls_Meta']),
                    'n': float(line_dict['effective_sample_size']),
                    'masks': []
                }
            line = f.readline().decode().strip()
    return cauchy_output


def get_full_phenotype_output(all_file, cauchy_output):
    with gzip.open(all_file, 'r') as f:
        header = f.readline().decode().strip()
        line = f.readline().decode().strip()
        while len(line) > 0:
            line_dict = dict(zip(header.split('\t'), line.split('\t')))
            mask = {
                'mask': line_dict['mask_name'],
                'mask_type': line_dict['mask_type'],
                'cases': optional_float(line_dict, 'n.cases_Meta'),
                'controls': optional_float(line_dict, 'n.controls_Meta'),
                'n': optional_float(line_dict, 'effective_sample_size'),
                'pValue': optional_float(line_dict, 'pValue'),
                'beta': optional_float(line_dict, 'beta'),
                'combinedAF': optional_float(line_dict, 'combinedAF'),
                'stdErr': optional_float(line_dict, 'stdErr')
            }
            cauchy_output[line_dict['gene']]['masks'].append(mask)
            line = f.readline().decode().strip()
    return cauchy_output


def get_output_with_lambda(output):
    lambda_rare = get_lambda(output, 'pValue_rare')
    lambda_low_freq = get_lambda(output, 'pValue_low_freq')
    for key in output:
        output[key]['lambda_rare'] = lambda_rare
        output[key]['lambda_low_freq'] = lambda_low_freq
    return output


def upload_output(ancestry, cohort, phecode, output):
    os.mkdir(phecode)
    with open(f'{phecode}/part-00000.json', 'w') as f:
        for line in output.values():
            f.write(f'{json.dumps(line)}\n')
    with open(f'{phecode}/metadata', 'w') as f:
        f.write(f'{{"name": "600k_600traits", "ancestry": "{ancestry}", "phenotype": "{phecode}"}}\n')
    outdir = f'{s3_out}/gene_associations/600k_600traits/{ancestry}/{cohort}/{phecode}'
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
    opts.add_argument('--ancestry', type=str, required=True)
    opts.add_argument('--cohort', type=str, required=True)

    # parse command line
    args = opts.parse_args()
    phecode = args.filename.split('.all_masks_formatted.')[0]
    all_file, cauchy_file = download_phecode_files(args.ancestry, args.cohort, phecode)
    cauchy_output = get_converted_phenotype_cauchy(args.ancestry, args.cohort, cauchy_file)
    phenotype_output = get_full_phenotype_output(all_file, cauchy_output)
    output_with_lambda = get_output_with_lambda(phenotype_output)
    upload_output(args.ancestry, args.cohort, phecode, output_with_lambda)
    os.remove(all_file)
    os.remove(cauchy_file)


if __name__ == '__main__':
    main()
