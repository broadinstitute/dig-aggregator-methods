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

s3_in = 's3://dig-analysis-data/gene_associations'
s3_out = 's3://dig-analysis-data/gene_associations/combined'


def get_file_path(data_type, phenotype):
    if data_type == '600trait':
        return f'{s3_in}/600k_combined/{phenotype}/'
    elif data_type == 'genebass':
        return f'{s3_in}/genebass/{phenotype}/'
    else:
        if phenotype == 'T2D':
            return f'{s3_in}/52k_T2D/{phenotype}/'
        else:
            return f'{s3_in}/52k_QT/{phenotype}/'


def get_gene_data(data_type, phenotype):
    file_path = get_file_path(data_type, phenotype)
    returncode = subprocess.run(
        ['aws', 's3', 'cp', file_path, f'./data/{data_type}/', '--recursive', '--exclude=metadata', '--exclude=_SUCCESS']
    ).returncode
    out = {}
    if returncode == 0:
        for file in glob.glob(f'./data/{data_type}/part-*'):
            print(f'Parsing {file}')
            with open(file, 'r') as f:
                for line in f.readlines():
                    json_line = json.loads(line.strip())
                    out[json_line['gene']] = {
                        'source': data_type,
                        'phenotype': phenotype,
                        'gene': json_line['gene'],
                        'pValue': json_line['pValue'] if 'pValue' in json_line else 1.0,
                        'beta': json_line['beta'] if 'beta' in json_line else 0.0,
                        'masks': json_line['masks']
                    }
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


def IVW(betas, std_errs):
    if len(betas) == 1:
        return betas[0], std_errs[0]
    else:
        weights = [1 / a / a for a in std_errs]
        if sum(weights) > 0:
            std_err = math.sqrt(1 / sum(weights))
            beta = sum([betas[i] * weights[i] for i in range(len(betas))]) / sum(weights)
            return beta, std_err
        else:
            return sum(betas) / len(betas), math.inf


def merge_masks(all_masks):
    grouped_masks = {}
    for mask in all_masks:
        if mask['mask'] not in grouped_masks:
            grouped_masks[mask['mask']] = []
        grouped_masks[mask['mask']].append(mask)
    merged_data = []
    for mask, masks in grouped_masks.items():
        betas = [mask['beta'] for mask in masks]
        std_errs = [mask['stdErr'] if mask['stdErr'] is not None and mask['stdErr'] > 0.0 else math.inf for mask in masks]
        beta, std_err = IVW(betas, std_errs)
        pValue = 2 * norm.cdf(-abs(beta) / std_err)
        merged_data.append({
            'mask': mask,
            'pValue': pValue if pValue > 0.0 else np.nextafter(0, 1),
            'beta': beta,
            'stdErr': std_err,
            'n': sum([mask['n'] for mask in masks]),
            'combinedAF': sum([mask['combinedAF'] for mask in masks]) / len(masks),
            'singleVariants': next(iter([mask['singleVariants'] for mask in masks if 'singleVariants' in mask]), None),
            'passingVariants': next(iter([mask['passingVariants'] for mask in masks if 'passingVariants' in mask]), None)
        })
    return merged_data


def to_gene_data_type_data(data):
    out = {}
    for datatype in ['AMP T2D-GENES', 'Broad CVDI 600 disease associations', 'Genebass']:
        for gene, gene_data in data[datatype].items():
            # for now only keep the first one that shows up
            if gene not in out:
                out[gene] = {datatype: gene_data}
    return out


def get_gene_level_data(gene_data):
    source = ' + '.join(list(gene_data.keys()))
    all_masks = merge_masks([mask for data_type_data in gene_data.values() for mask in data_type_data['masks']])
    if len(all_masks) > 0:
        min_p_mask = sorted(all_masks, key=lambda mask: mask['pValue'])[0]
        return all_masks, minp([a['pValue'] for a in all_masks]), min_p_mask['beta'], source
    else:
        min_p_gene_data = sorted(gene_data.values(), key=lambda gene_data: gene_data['pValue'])[0]
        return all_masks, minp([a['pValue'] for a in gene_data.values()]), min_p_gene_data['beta'], source


def merge(phenotype, data):
    gene_data_type_data = to_gene_data_type_data(data)
    merged_data = {}
    for gene, gene_data in gene_data_type_data.items():
        all_masks, pValue, beta, source = get_gene_level_data(gene_data)
        merged_data[gene] = {
            'source': source,
            'phenotype': phenotype,
            'gene': gene,
            'pValue': pValue if pValue > 0.0 else np.nextafter(0, 1),
            'beta': beta,
            'masks': all_masks
        }
    return merged_data


def output_and_delete_data(phenotype, data):
    with open('part-00000.json', 'w') as f:
        for gene_data in data.values():
            f.write('{}\n'.format(json.dumps(gene_data)))
    subprocess.check_call(['aws', 's3', 'cp', 'part-00000.json', f'{s3_out}/{phenotype}/'])
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', f'_SUCCESS', f'{s3_out}/{phenotype}/_SUCCESS'])
    os.remove('_SUCCESS')
    os.remove('part-00000.json')
    shutil.rmtree('./data')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    args = parser.parse_args()
    phenotype = args.phenotype

    data = {
        'Broad CVDI 600 disease associations': get_gene_data('600trait', phenotype),
        'AMP T2D-GENES': get_gene_data('52k', phenotype),
        'Genebass': get_gene_data('genebass', phenotype)
    }
    merged_data = merge(phenotype, data)
    output_and_delete_data(phenotype, merged_data)


if __name__ == '__main__':
    main()
