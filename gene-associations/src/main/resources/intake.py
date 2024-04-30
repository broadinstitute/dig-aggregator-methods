#!/usr/bin/python3

import argparse
import gzip
import json
import os
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_phenotype_file(path):
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/gene_associations_raw/{path}/mask_results.tsv.gz', '.'])


def get_metadata(path):
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/gene_associations_raw/{path}/metadata', '.'])
    with open('metadata', 'r') as f:
        data = json.load(f)
    os.remove('metadata')
    return data


def eff_n(metadata):
    if metadata['dichotomous']:
        return 4.0 / ((1.0 / metadata['cases']) + (1.0 / metadata['controls']))
    else:
        return metadata['effective_sample_size']


def add_optional_field(mask, field, field_out, typ, default=None):
    if field != '':
        mask[field_out] = typ(field)
    elif default is not None:
        mask[field_out] = default
    return mask


def get_converted_mask_output(test_type, dataset, phenotype, mask_col_map, default_n):
    output = {}
    with gzip.open('mask_results.tsv.gz', 'r') as f:
        header = f.readline().decode().strip()
        line = f.readline().decode().strip()
        while len(line) > 0:
            line_dict = dict(zip(header.split('\t'), line.split('\t')))
            gene = line_dict[mask_col_map['gene']]
            if gene not in output:
                output[gene] = {
                    'test_type': test_type,
                    'dataset': dataset,
                    'phenotype': phenotype,
                    'gene': gene,
                    'masks': []
                }
            mask = {'mask': line_dict[mask_col_map['mask']]}
            mask = add_optional_field(mask, line_dict.get(mask_col_map['pValue'], ''), 'pValue', float)
            mask = add_optional_field(mask, line_dict.get(mask_col_map['beta'], ''), 'beta', float)
            mask = add_optional_field(mask, line_dict.get(mask_col_map['combinedAF'], ''), 'combinedAF', float)
            mask = add_optional_field(mask, line_dict.get(mask_col_map['stdErr'], ''), 'stdErr', float)
            mask = add_optional_field(mask, line_dict.get(mask_col_map['n'], ''), 'n', float, default_n)
            mask = add_optional_field(mask, line_dict.get(mask_col_map['singleVariants'], ''), 'singleVariants', int)
            mask = add_optional_field(mask, line_dict.get(mask_col_map['passingVariants'], ''), 'passingVariants', int)

            output[gene]['masks'].append(mask)
            line = f.readline().decode().strip()
    os.remove('mask_results.tsv.gz')
    return output


def upload_output(path, output):
    with open(f'part-00000.json', 'w') as f:
        for line in output.values():
            f.write(f'{json.dumps(line)}\n')
    test_type, dataset, phenotype = path.split('/')
    path_out = f'{s3_out}/gene_associations/intake/{phenotype}/{test_type}/{dataset}'
    subprocess.check_call(['aws', 's3', 'cp', f'part-00000.json', f'{path_out}/part-00000.json'])
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', f'{path_out}/_SUCCESS'])
    os.remove('part-00000.json')
    os.remove('_SUCCESS')


def main():
    """
    Arguments:  filename
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('--path', type=str, required=True)

    # parse command line
    args = opts.parse_args()
    path = args.path
    test_type, dataset, phenotype = args.path.split('/')
    download_phenotype_file(path)
    metadata = get_metadata(path)
    converted_output = get_converted_mask_output(test_type, dataset, phenotype, metadata['column_map_mask'], eff_n(metadata))
    upload_output(path, converted_output)


if __name__ == '__main__':
    main()
