#!/usr/bin/python3

import argparse
import gzip
import json
import os
import re
import subprocess

convert_mask = {
    'LoF_HC': 'LoF_HC',
    'LoF_HC|missense|LC': 'LoF_HC|missense|LC',
    'missense|LC': 'missense|LC'
}

src_dir = 's3://dig-analysis-data/gene_associations_raw/genebass'
out_dir = 's3://dig-analysis-data/gene_associations/genebass'


def download_phenotype_file(filename):
    subprocess.check_call(['aws', 's3', 'cp', f'{src_dir}/{filename}', filename])


def add_optional_field(mask, field, field_out, typ, default=None):
    if field != '':
        mask[field_out] = typ(field)
    elif default is not None:
        mask[field_out] = default
    return mask


def get_converted_mask_output(filename):
    output = {}
    with gzip.open(filename, 'r') as f:
        header = f.readline().decode().strip()
        line = f.readline().decode().strip()
        while len(line) > 0:
            line_dict = dict(zip(header.split('\t'), line.split('\t')))
            if line_dict['gene'] not in output:
                output[line_dict['gene']] = {
                    'dataset': 'genebass',
                    'phenotype': line_dict['portal_pheno'],
                    'ensemblId': line_dict['Gene_stable_ID'],
                    'gene': line_dict['gene'],
                    'masks': []
                }
            if line_dict['mask'] in convert_mask and 'pValue' in line_dict and line_dict['pValue'] != '':
                mask = {
                    'mask': convert_mask[line_dict['mask']],
                    'pValue': float(line_dict['pValue']),
                    'beta': float(line_dict['beta']),
                    'combinedAF': float(line_dict['combinedAF']),
                    'stdErr': float(line_dict['stdErr']) if line_dict['stdErr'] != 'NA' else None,
                }
                mask = add_optional_field(mask, line_dict.get('n_effective', ''), 'n', float, 0.0)
                mask = add_optional_field(mask, line_dict.get('n_cases', ''), 'cases', float)
                mask = add_optional_field(mask, line_dict.get('n_controls', ''), 'controls', float)
                mask = add_optional_field(mask, line_dict.get('singleVariants', ''), 'singleVariants', int)
                mask = add_optional_field(mask, line_dict.get('passingVariants', ''), 'passingVariants', int)

                output[line_dict['gene']]['masks'].append(mask)
            line = f.readline().decode().strip()
    return output


def upload_output(filename, output):
    phenotype = re.findall('masks_formatted.(.*).gene_results.tsv.gz', filename)[0]
    os.mkdir(phenotype)
    with open(f'./{phenotype}/part-00000.json', 'w') as f:
        for line in output.values():
            f.write(f'{json.dumps(line)}\n')
    with open(f'./{phenotype}/metadata', 'w') as f:
        f.write(f'{{"name": "genebass", "phenotype": "{phenotype}"}}\n')
    subprocess.check_call(['aws', 's3', 'cp', f'{phenotype}/part-00000.json', f'{out_dir}/{phenotype}/part-00000.json'])
    subprocess.check_call(['aws', 's3', 'cp', f'{phenotype}/metadata', f'{out_dir}/{phenotype}/metadata'])
    os.remove(f'{phenotype}/part-00000.json')
    os.remove(f'{phenotype}/metadata')
    os.rmdir(phenotype)


def main():
    """
    Arguments:  filename
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('--filename', type=str, required=True)

    # parse command line
    args = opts.parse_args()
    filename = args.filename
    download_phenotype_file(filename)
    converted_output = get_converted_mask_output(filename)
    upload_output(filename, converted_output)
    os.remove(filename)


if __name__ == '__main__':
    main()
