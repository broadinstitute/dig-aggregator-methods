#!/usr/bin/python3
import argparse
import glob
import json
import math
import numpy as np
import os
import re
from scipy.stats import t as tdist
import shutil
import subprocess

s3_in = 's3://dig-analysis-data/out/ldsc/staging/partitioned_heritability'
s3_path = 's3://dig-analysis-data/out/ldsc/partitioned_heritability'


def get_phenotype_annot_ancestries(phenotype):
    subprocess.check_call(['aws', 's3', 'cp',
                           f'{s3_in}/{phenotype}/', f'./{phenotype}',
                           '--recursive', '--exclude=_SUCCESS'])
    out = {}
    for file in glob.glob(f'./{phenotype}/*/*/*'):
        results_search = re.findall('./[^/]+/[^/]+/(.+)/(.+).results', file)
        if len(results_search) > 0:
            sub_region, result = results_search[0]
            split_result = result.split('.')
            ancestry, phenotype, annot = split_result[0], split_result[1], '.'.join(split_result[2:])
            if phenotype not in out:
                out[phenotype] = {}
            if annot not in out[phenotype]:
                out[phenotype][annot] = {}
            if sub_region not in out[phenotype][annot]:
                out[phenotype][annot][sub_region] = []
            out[phenotype][annot][sub_region].append(ancestry)
    return out


def translate(file):
    with open(file, 'r') as f:
        # Get last line of file
        header = [a.strip() for a in f.readline().strip().split('\t')]
        last_line = f.readline()
        next_line = f.readline()
        while len(next_line) > 0:
            last_line = next_line
            next_line = f.readline()
        last_line = [a.strip() for a in last_line.strip().split('\t')]
        data_dict = dict(zip(header, last_line))
        return {
            'snps': float(data_dict['Prop._SNPs']),
            'h2': {'beta': float(data_dict['Prop._h2']), 'stdErr': float(data_dict['Prop._h2_std_error'])},
            'enrichment': {'beta': float(data_dict['Enrichment']), 'stdErr': float(data_dict['Enrichment_std_error'])},
            'coefficient': {'beta': float(data_dict['Coefficient']), 'stdErr': float(data_dict['Coefficient_std_error'])},
            'diff': {'beta': float(data_dict['Diff']), 'stdErr': float(data_dict['Diff_std_error'])},
            'pValue': float(data_dict['Enrichment_p'])
        }


def get_data(phenotype, annot_map):
    out = {}
    for annot, sub_region_map in annot_map.items():
        for sub_region, ancestries in sub_region_map.items():
            out[annot] = {}
            for ancestry in ancestries:
                file = f'{phenotype}/ancestry={ancestry}/{sub_region}/{ancestry}.{phenotype}.{annot}.results'
                out[annot][ancestry] = translate(file)
    return out


def fold_data(data, other_data):
    w_data = 1.0 / data['stdErr'] / data['stdErr']
    w_other = 1.0 / other_data['stdErr'] / other_data['stdErr']
    stdErr = math.sqrt(1.0 / (w_data + w_other))
    beta = (data['beta'] * w_data + other_data['beta'] * w_other) / (w_data + w_other)
    return {'beta': beta, 'stdErr': stdErr}


def pValue(beta, stdErr):
    p = 2 * tdist.sf(abs(beta / stdErr), 200)  # 200 is the number of jackknife blocks used in the partitioning
    return p if p > 0 else np.nextafter(0, 1)


def meta_analyze(data):
    for annot in data:
        data_to_meta_analyze = [output_data for ancestry, output_data in data[annot].items() if ancestry != 'Mixed']
        if len(data_to_meta_analyze) > 0:
            maybe_meta_data = data_to_meta_analyze[0].copy()
            for other_non_mixed_data in data_to_meta_analyze[1:]:
                maybe_meta_data['h2'] = fold_data(maybe_meta_data['h2'], other_non_mixed_data['h2'])
                maybe_meta_data['enrichment'] = fold_data(maybe_meta_data['enrichment'], other_non_mixed_data['enrichment'])
                maybe_meta_data['coefficient'] = fold_data(maybe_meta_data['coefficient'], other_non_mixed_data['coefficient'])
                maybe_meta_data['diff'] = fold_data(maybe_meta_data['diff'], other_non_mixed_data['diff'])
                maybe_meta_data['pValue'] = pValue(maybe_meta_data['diff']['beta'], maybe_meta_data['diff']['stdErr'])
                maybe_meta_data['snps'] = maybe_meta_data['h2']['beta'] / maybe_meta_data['enrichment']['beta']
            if 'Mixed' not in data[annot] or maybe_meta_data['enrichment']['stdErr'] < data[annot]['Mixed']['enrichment']['stdErr']:
                data[annot]['Mixed'] = maybe_meta_data
    return data


def upload_data(phenotype, data):
    file = f'./{phenotype}.json'
    with open(file, 'w') as f:
        for annot, ancestry_data in data.items():
            split_annot = annot.split('___')
            if len(split_annot) == 2:
                annotation, tissue = split_annot
                biosample = None
            else:
                annotation, tissue, biosample = split_annot
            for ancestry, output_data in ancestry_data.items():
                formatted_data = {
                    'phenotype': phenotype,
                    'annotation': annotation,
                    'tissue': tissue.replace('_', ' '),
                    'biosample': biosample,
                    'ancestry': ancestry,
                    'SNPs': output_data['snps'],
                    'h2_beta': output_data['h2']['beta'],
                    'h2_stdErr': output_data['h2']['stdErr'],
                    'enrichment_beta': output_data['enrichment']['beta'],
                    'enrichment_stdErr': output_data['enrichment']['stdErr'],
                    'coefficient_beta': output_data['coefficient']['beta'],
                    'coefficient_stdErr': output_data['coefficient']['stdErr'],
                    'diff_beta': output_data['diff']['beta'],
                    'diff_stdErr': output_data['diff']['stdErr'],
                    'pValue': output_data['pValue']
                }
                f.write(json.dumps(formatted_data) + '\n')
    subprocess.check_call(['aws', 's3', 'cp', file, f'{s3_path}/{phenotype}/'])
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', f'{s3_path}/{phenotype}/'])
    os.remove(file)
    os.remove('_SUCCESS')
    shutil.rmtree(f'./{phenotype}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', type=str, required=True, help="Phenotype to translate (e.g. T2D)")
    args = parser.parse_args()

    phenotype = args.phenotype

    phenotype_annot_ancestries = get_phenotype_annot_ancestries(phenotype)
    for phenotype, annot_map in phenotype_annot_ancestries.items():
        data = get_data(phenotype, annot_map)
        data = meta_analyze(data)
        upload_data(phenotype, data)


if __name__ == '__main__':
    main()
