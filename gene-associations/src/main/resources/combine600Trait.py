#!/usr/bin/python3

import json
import math
import os
from scipy.stats import norm
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def get_trait_to_phecodes():
    phecode_file = 's3://dig-analysis-data/bin/gene_association/phecode_map.json'
    subprocess.check_call(['aws', 's3', 'cp', phecode_file, './'])
    with open('phecode_map.json', 'r') as f:
        trait_to_phecodes = json.load(f)
    return trait_to_phecodes


def get_phecode(phenotype, phecode):
    path_in = f'{s3_in}/gene_associations/600k_600traits/Mixed/UKB_450k_AoU_250k_MGB_53k_META_overlapcorrected'
    file = f'{path_in}/{phecode}/part-00000.json'
    subprocess.check_call(['aws', 's3', 'cp', file, './'])
    out = {}
    with open('part-00000.json', 'r') as f:
        for line in f.readlines():
            json_line = json.loads(line.strip())
            if json_line['pValue_rare'] is not None and json_line['beta'] is not None:
                out[json_line['gene']] = {
                    'phenotype': phenotype,
                    'gene': json_line['gene'],
                    'pValue': json_line['pValue_rare'],
                    'beta': json_line['beta'],
                    'masks': json_line['masks']
                }
    os.remove('part-00000.json')
    return out


def cauchy(p_values):
    w = 1 / len(p_values)
    if min(p_values) < 1E-15:
        return 1 / w / sum([math.cos(p) / math.sin(p) for p in p_values]) / math.pi
    else:
        return 0.5 - math.atan(sum([w * math.tan(math.pi * (0.5 - p)) for p in p_values])) / math.pi


def merge_masks(all_masks):
    grouped_masks = {}
    for mask in all_masks:
        if mask['pValue'] is not None and mask['beta'] is not None:
            if mask['mask'] not in grouped_masks:
                grouped_masks[mask['mask']] = []
            grouped_masks[mask['mask']].append(mask)
    merged_data = []
    for mask, masks in grouped_masks.items():
        min_p_masks = sorted(masks, key=lambda x: x['pValue'] if x['pValue'] is not None else 1.0)[0]
        mask_p_values = [a['pValue'] for a in masks if a['pValue'] is not None]
        new_pValue = cauchy(mask_p_values) if len(mask_p_values) > 0 else 1.0
        new_beta = min_p_masks['beta']
        new_stdErr = -abs(new_beta) / norm.ppf(new_pValue / 2) if new_beta != 0.0 else 1.0
        merged_data.append({
            'mask': mask,
            'pValue': new_pValue,
            'beta': new_beta,
            'stdErr': new_stdErr,
            'n': min_p_masks['n'],
            'combinedAF': min_p_masks['combinedAF']
        })
    return merged_data


def to_gene_phecode_data(data):
    out = {}
    for phecode, phecode_data in data.items():
        for gene, gene_data in phecode_data.items():
            if gene not in out:
                out[gene] = {}
            out[gene][phecode] = gene_data
    return out


def merge(phenotype, data):
    gene_phecode_data = to_gene_phecode_data(data)
    merged_data = {}
    for gene, gene_data in gene_phecode_data.items():
        all_masks = [mask for phecode_data in gene_data.values() for mask in phecode_data['masks']]
        min_p_mask = sorted(all_masks, key=lambda mask: mask['pValue'] if mask['pValue'] is not None else 1.0)[0]
        mask_p_values = [a['pValue'] for a in all_masks if a['pValue'] is not None]
        merged_data[gene] = {
            'phenotype': phenotype,
            'gene': gene,
            'pValue': cauchy(mask_p_values) if len(mask_p_values) > 0 else 1.0,
            'beta': min_p_mask['beta'],
            'masks': merge_masks(all_masks)
        }
    return merged_data


def output_and_delete_data(phenotype, data):
    path_out = f'{s3_out}/gene_associations/600k_combined'
    with open('part-00000.json', 'w') as f:
        for gene_data in data.values():
            f.write('{}\n'.format(json.dumps(gene_data)))
    subprocess.check_call(['aws', 's3', 'cp', 'part-00000.json', f'{path_out}/{phenotype}/'])
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', f'_SUCCESS', f'{path_out}/{phenotype}/_SUCCESS'])
    os.remove('_SUCCESS')
    os.remove('part-00000.json')


def main():
    trait_to_phecodes = get_trait_to_phecodes()
    for phenotype, phecodes in trait_to_phecodes.items():
        data = {}
        for phecode in phecodes:
            data[phecode] = get_phecode(phenotype, phecode)
        merged_data = merge(phenotype, data)
        output_and_delete_data(phenotype, merged_data)


if __name__ == '__main__':
    main()
