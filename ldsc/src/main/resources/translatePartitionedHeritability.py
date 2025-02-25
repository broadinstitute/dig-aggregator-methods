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

this_project = os.environ['PROJECT']
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def get_annot_map(project, phenotype):
    path_in = f'{s3_in}/out/ldsc/staging/partitioned_heritability/{project}/{phenotype}/'
    subprocess.check_call(
        f'aws s3 cp {path_in} ./{phenotype}/{project}/ --recursive --exclude="*_SUCCESS"',
        shell=True
    )
    out = {}
    for file in glob.glob(f'./{phenotype}/{project}/*/*/*/*'):
        results_search = re.findall('./[^/]+/[^/]+/[^/]+/(.+)/[^/]+/(.+).results', file)
        if len(results_search) > 0:
            sub_region, result = results_search[0]
            ancestry, _, annotation, tissue = result.split('.')
            annot = f'{annotation}.{tissue}'
            if sub_region not in out:
                out[sub_region] = {}
            if annot not in out[sub_region]:
                out[sub_region][annot] = []
            out[sub_region][annot].append(ancestry)
    return out


def translate(file):
    out = {}
    with open(file, 'r') as f:
        header = [a.strip() for a in f.readline().strip().split('\t')]
        # Read up to lines with entries ending in '_0'
        line = f.readline()
        split_line = [a.strip() for a in line.strip().split('\t')]
        while int(split_line[0].rsplit('_', 1)[-1]) == 0:
            line = f.readline()
            split_line = [a.strip() for a in line.strip().split('\t')]
        maybe_confounder_key = split_line[0].rsplit('_', 1)[0]
        while len(line) > 0:
            data_dict = dict(zip(header, split_line))
            if float(data_dict['Prop._h2_std_error']) > 0.0:
                out[split_line[0].rsplit('_', 1)[0]] = {
                    'snps': float(data_dict['Prop._SNPs']),
                    'h2': {'beta': float(data_dict['Prop._h2']), 'stdErr': float(data_dict['Prop._h2_std_error'])},
                    'enrichment': {'beta': float(data_dict['Enrichment']), 'stdErr': float(data_dict['Enrichment_std_error'])},
                    'coefficient': {'beta': float(data_dict['Coefficient']), 'stdErr': float(data_dict['Coefficient_std_error'])},
                    'diff': {'beta': float(data_dict['Diff']), 'stdErr': float(data_dict['Diff_std_error'])},
                    'pValue': float(data_dict['Enrichment_p'])
                }
            line = f.readline()
            split_line = [a.strip() for a in line.strip().split('\t')]
        # Usually the first line is the annotation variable which is unneeded
        if len(out) > 1:
            out.pop(maybe_confounder_key)
    return out


def get_data(project, phenotype, full_map):
    out = {}
    for sub_region, annot_map in full_map.items():
        out[sub_region] = {}
        for annot, ancestries in annot_map.items():
            annotation, tissue = annot.split('.')
            out[sub_region][annot] = {}
            for ancestry in ancestries:
                file = f'{phenotype}/{project}/ancestry={ancestry}/{sub_region}/{annotation}___{tissue}/{ancestry}.{phenotype}.{annot}.results'
                translated_file = translate(file)
                for biosample, biosample_data in translated_file.items():
                    if biosample not in out[sub_region][annot]:
                        out[sub_region][annot][biosample] = {}
                    out[sub_region][annot][biosample][ancestry] = biosample_data
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
    for sub_region in data:
        for annot in data[sub_region]:
            for biosample in data[sub_region][annot]:
                data_to_meta_analyze = [output_data for ancestry, output_data in data[sub_region][annot][biosample].items() if ancestry != 'Mixed']
                if len(data_to_meta_analyze) > 0:
                    maybe_meta_data = data_to_meta_analyze[0].copy()
                    for other_non_mixed_data in data_to_meta_analyze[1:]:
                        maybe_meta_data['h2'] = fold_data(maybe_meta_data['h2'], other_non_mixed_data['h2'])
                        maybe_meta_data['enrichment'] = fold_data(maybe_meta_data['enrichment'], other_non_mixed_data['enrichment'])
                        maybe_meta_data['coefficient'] = fold_data(maybe_meta_data['coefficient'], other_non_mixed_data['coefficient'])
                        maybe_meta_data['diff'] = fold_data(maybe_meta_data['diff'], other_non_mixed_data['diff'])
                        maybe_meta_data['pValue'] = pValue(maybe_meta_data['diff']['beta'], maybe_meta_data['diff']['stdErr'])
                        maybe_meta_data['snps'] = maybe_meta_data['h2']['beta'] / maybe_meta_data['enrichment']['beta']
                    if 'Mixed' not in data[sub_region][annot][biosample] or \
                            maybe_meta_data['enrichment']['stdErr'] < data[sub_region][annot][biosample]['Mixed']['enrichment']['stdErr']:
                        data[sub_region][annot][biosample]['Mixed'] = maybe_meta_data
    return data


def upload_data(phenotype, data):
    file = f'./{phenotype}.json'
    with open(file, 'w') as f:
        for project in data:
            for sub_region in data[project]:
                for annot in data[project][sub_region]:
                    for biosample in data[project][sub_region][annot]:
                        write_biosample = biosample if sub_region == 'annotation-tissue-biosample' else None
                        annotation, tissue = annot.split('.')
                        for ancestry, output_data in data[project][sub_region][annot][biosample].items():
                            formatted_data = {
                                'project': project,
                                'phenotype': phenotype,
                                'annotation': annotation,
                                'tissue': tissue,
                                'biosample': write_biosample,
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
    subprocess.check_call(['aws', 's3', 'cp', file, f'{s3_out}/out/ldsc/partitioned_heritability/{phenotype}/'])
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', f'{s3_out}/out/ldsc/partitioned_heritability/{phenotype}/'])
    os.remove(file)
    os.remove('_SUCCESS')
    shutil.rmtree(f'./{phenotype}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', type=str, required=True, help="Phenotype to translate (e.g. T2D)")
    args = parser.parse_args()

    phenotype = args.phenotype

    data = {}
    for project in {'portal', this_project}:
        annot_map = get_annot_map(project, phenotype)
        data[project] = get_data(project, phenotype, annot_map)
        data[project] = meta_analyze(data[project])
    upload_data(phenotype, data)


if __name__ == '__main__':
    main()
