#!/usr/bin/python3

import argparse
import json
import glob
import math
import os
import subprocess

s3_in = 's3://dig-analysis-data/gene_associations'
s3_out = 's3://dig-analysis-data/gene_associations/combined'


def get_file_path(data_type, phenotype):
    if data_type == '600trait':
        return f'{s3_in}/600k_combined/{phenotype}/'
    else:
        if phenotype == 'T2D':
            return f'{s3_in}/52k_T2D/{phenotype}/'
        else:
            return f'{s3_in}/52k_QT/{phenotype}/'


def get_gene_data(data_type, phenotype):
    file_path = get_file_path(data_type, phenotype)
    returncode = subprocess.run(
        ['aws', 's3', 'cp', file_path, './', '--recursive', '--exclude=metadata', '--exclude=_SUCCESS']
    ).returncode
    out = {}
    if returncode == 0:
        for file in glob.glob('./part-*'):
            print(f'Parsing {file}')
            with open(file, 'r') as f:
                for line in f.readlines():
                    json_line = json.loads(line.strip())
                    out[json_line['gene']] = {
                        'phenotype': phenotype,
                        'gene': json_line['gene'],
                        'pValue': json_line['pValue'],
                        'beta': json_line['beta'],
                        'masks': json_line['masks']
                    }
            os.remove(file)
    return out


def cauchy(p_values):
    w = 1 / len(p_values)
    return 0.5 - math.atan(sum([w * math.tan(math.pi * (0.5 - p)) for p in p_values])) / math.pi


def IVW(betas, std_errs):
    if len(betas) == 1:
        return betas[0], std_errs[0]
    else:
        weights = [1 / a / a if a is not None else 0 for a in std_errs]
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
        std_errs = [mask['stdErr'] if mask['stdErr'] is not None else math.inf for mask in masks]
        beta, std_err = IVW(betas, std_errs)
        merged_data.append({
            'mask': mask,
            'pValue': cauchy([a['pValue'] for a in masks]),
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
    for data_type, data_type_data in data.items():
        for gene, gene_data in data_type_data.items():
            if gene not in out:
                out[gene] = {}
            out[gene][data_type] = gene_data
    return out


def get_gene_level_data(gene_data):
    all_masks = merge_masks([mask for data_type_data in gene_data.values() for mask in data_type_data['masks']])
    if len(all_masks) > 0:
        min_p_mask = sorted(all_masks, key=lambda mask: mask['pValue'])[0]
        return all_masks, cauchy([a['pValue'] for a in all_masks]), min_p_mask['beta']
    else:
        min_p_gene_data = sorted(gene_data.values(), key=lambda gene_data: gene_data['pValue'])[0]
        return all_masks, cauchy([a['pValue'] for a in gene_data.values()]), min_p_gene_data['beta']


def merge(phenotype, data):
    gene_data_type_data = to_gene_data_type_data(data)
    merged_data = {}
    for gene, gene_data in gene_data_type_data.items():
        all_masks, pValue, beta = get_gene_level_data(gene_data)
        merged_data[gene] = {
            'phenotype': phenotype,
            'gene': gene,
            'pValue': pValue,
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    args = parser.parse_args()
    phenotype = args.phenotype

    data = {
        '600trait': get_gene_data('600trait', phenotype),
        '52k': get_gene_data('52k', phenotype)
    }
    merged_data = merge(phenotype, data)
    output_and_delete_data(phenotype, merged_data)


if __name__ == '__main__':
    main()
