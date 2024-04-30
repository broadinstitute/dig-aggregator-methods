#!/usr/bin/python3

import argparse
import json
import glob
import math
import numpy as np
import os
from scipy.stats import norm
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def get_gene_data(phenotype):
    file_path = f'{s3_in}/gene_associations/intake/{phenotype}/'
    returncode = subprocess.run(
        ['aws', 's3', 'cp', file_path, f'./data/{phenotype}/', '--recursive', '--exclude="*_SUCCESS"']
    ).returncode
    out = {}
    if returncode == 0:
        for file in glob.glob(f'./data/{phenotype}/*/*/part-*'):
            print(f'Parsing {file}')
            with open(file, 'r') as f:
                for line in f.readlines():
                    json_line = json.loads(line.strip())
                    if json_line['gene'] not in out:
                        out[json_line['gene']] = []
                    out[json_line['gene']].append({
                        'test_type': json_line['test_type'],
                        'phenotype': json_line['phenotype'],
                        'dataset': json_line['dataset'],
                        'gene': json_line['gene'],
                        'masks': json_line['masks']
                    })
            os.remove(file)
    return out


# 1 - eps = 1.0 (and minp = 0.0) if eps < 1E-16
# 1 - (1 - eps)^N ~ 1 - 1 + N * eps - HOT(eps^2) -> N * eps in the limit eps -> 0
# https://en.wikipedia.org/wiki/%C5%A0id%C3%A1k_correction
def minp(p_values):
    N = len(p_values)
    min_p = min(p_values)
    if min_p < 1E-15:
        return N * min_p
    else:
        return 1 - (1 - min(p_values))**N


def IVW(masks):
    if len(masks) == 0:
        return None, None
    elif len(masks) == 1:
        return masks[0]['beta'], masks[0]['stdErr']
    else:
        weights = [1 / mask['stdErr'] / mask['stdErr'] for mask in masks]
        if sum(weights) > 0:
            std_err = math.sqrt(1 / sum(weights))
            beta = sum([masks[i]['beta'] * weights[i] for i in range(len(masks))]) / sum(weights)
            return beta, std_err
        else:
            return None, None


def merge_masks(all_masks):
    grouped_masks = {}
    for mask in all_masks:
        if mask['mask'] not in grouped_masks:
            grouped_masks[mask['mask']] = []
        grouped_masks[mask['mask']].append(mask)
    merged_data = []
    for mask, masks in grouped_masks.items():
        filtered_masks = [mask for mask in masks if 'beta' in mask and mask['beta'] is not None and
                          'stdErr' in mask and mask['stdErr'] is not None and mask['stdErr'] > 0.0]
        beta, std_err = IVW(filtered_masks)
        if beta is not None and std_err is not None:
            pValue = 2 * norm.cdf(-abs(beta) / std_err)
        else:
            pValue = sorted(masks, key=lambda mask: mask['pValue'])[0]
        filtered_combined = [mask['combinedAF'] for mask in masks if 'combinedAF' in mask and mask['combinedAF'] is not None]
        filtered_n = [mask['n'] for mask in masks if 'n' in mask and mask['n'] is not None]
        merged_data.append({
            'mask': mask,
            'pValue': pValue if pValue > 0.0 else np.nextafter(0, 1),
            'beta': beta,
            'stdErr': std_err,
            'n': sum(filtered_n) if len(filtered_n) > 0 else None,
            'combinedAF': sum(filtered_combined) / len(filtered_combined) if len(filtered_combined) > 0 else None,
            'singleVariants': next(iter([mask['singleVariants'] for mask in masks if 'singleVariants' in mask]), None),
            'passingVariants': next(iter([mask['passingVariants'] for mask in masks if 'passingVariants' in mask]), None)
        })
    return merged_data


def get_gene_level_data(gene_data):
    all_masks = merge_masks([mask for data_type_data in gene_data for mask in data_type_data['masks']])
    min_p_mask = sorted(all_masks, key=lambda mask: mask['pValue'])[0]
    return all_masks, minp([a['pValue'] for a in all_masks]), min_p_mask['beta']


def merge(phenotype, data):
    merged_data = {}
    for gene, gene_data in data.items():
        all_masks, pValue, beta = get_gene_level_data(gene_data)
        merged_data[gene] = {
            'phenotype': phenotype,
            'gene': gene,
            'pValue': pValue if pValue > 0.0 else np.nextafter(0, 1),
            'beta': beta,
            'masks': all_masks
        }
    return merged_data


def output_and_delete_data(phenotype, data):
    file_path = f'{s3_out}/gene_associations/combined/{phenotype}'
    with open('part-00000.json', 'w') as f:
        for gene_data in data.values():
            f.write('{}\n'.format(json.dumps(gene_data)))
    subprocess.check_call(['aws', 's3', 'cp', 'part-00000.json', f'{file_path}/'])
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', f'_SUCCESS', f'{file_path}/_SUCCESS'])
    os.remove('_SUCCESS')
    os.remove('part-00000.json')
    shutil.rmtree('./data')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    args = parser.parse_args()
    phenotype = args.phenotype

    data = get_gene_data(phenotype)
    merged_data = merge(phenotype, data)
    output_and_delete_data(phenotype, merged_data)


if __name__ == '__main__':
    main()
