#!/usr/bin/python3
import glob
import json
import math
import numpy as np
import os
import re
from scipy.stats import norm
import subprocess

downloaded_files = '/mnt/var/ldsc'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def get_phenotype_project_ancestries():
    out = {}
    for file in glob.glob(f'{downloaded_files}/*/*/*.log'):
        ancestry, project, phenotype_ancestry = \
            re.findall(f'{downloaded_files}/ancestry=([^/]*)/([^/]*)/([^/]*).log', file)[0]
        phenotype = re.findall(f'(.*)_{ancestry}', phenotype_ancestry)[0]
        if phenotype not in out:
            out[phenotype] = {}
        if project not in out[phenotype]:
            out[phenotype][project] = []
        out[phenotype][project].append(ancestry)
    return out


def translate(ancestry, file):
    out = {}
    with open(file, 'r') as f:
        # Find the summary section of the log
        line = f.readline().strip()
        while line != 'Summary of Genetic Correlation Results':
            line = f.readline().strip()

        header = [h for h in re.sub(' +', ' ', f.readline().strip()).split(' ')]
        line = f.readline().strip()
        while len(line) > 0:
            data_dict = {h: col for h, col in zip(header, re.sub(' +', ' ', line).split(' '))}
            if data_dict['rg'] != 'NaN' and data_dict['rg'] != 'None':
                other_phenotype = re.findall(f'.*/(.+)_{ancestry}.sumstats.gz', data_dict['p2'])[0]
                out[other_phenotype] = {
                    'rg': float(data_dict['rg']),
                    'stdErr': float(data_dict['se']),
                    'pValue': float(data_dict['p']) if float(data_dict['p']) > 0 else np.nextafter(0, 1)
                }
            line = f.readline().strip()
    return out


def get_data(phenotype, project, ancestries):
    out = {}
    for ancestry in ancestries:
        file = f'{downloaded_files}/ancestry={ancestry}/{project}/{phenotype}_{ancestry}.log'
        data = translate(ancestry, file)
        for other_phenotype in data:
            if other_phenotype not in out:
                out[other_phenotype] = {}
            out[other_phenotype][ancestry] = data[other_phenotype]
    return out


def fold_data(data, other_data):
    w_data = 1.0 / data['stdErr'] / data['stdErr']
    w_other = 1.0 / other_data['stdErr'] / other_data['stdErr']
    se = math.sqrt(1.0 / (w_data + w_other))
    rg = (data['rg'] * w_data + other_data['rg'] * w_other) / (w_data + w_other)
    p = 2 * norm.cdf(-abs(rg / se))
    return {'rg': rg, 'stdErr': se, 'pValue': p if p > 0 else np.nextafter(0, 1)}


def meta_analyze(data):
    for phenotype in data:
        data_to_meta_analyze = [output_data for ancestry, output_data in data[phenotype].items() if ancestry != 'Mixed']
        if len(data_to_meta_analyze) > 0:
            maybe_meta_data = data_to_meta_analyze[0]
            for other_non_mixed_data in data_to_meta_analyze[1:]:
                maybe_meta_data = fold_data(maybe_meta_data, other_non_mixed_data)
            if 'Mixed' not in data[phenotype] or maybe_meta_data['stdErr'] < data[phenotype]['Mixed']['stdErr']:
                data[phenotype]['Mixed'] = maybe_meta_data
    return data


def upload_data(phenotype, data):
    file = f'./{phenotype}.json'
    with open(file, 'w') as f:
        for project, project_data in data.items():
            for other_phenotype, ancestry_data in project_data.items():
                for ancestry, output_data in ancestry_data.items():
                    output_data['phenotype'] = phenotype
                    output_data['other_phenotype'] = other_phenotype
                    output_data['ancestry'] = ancestry
                    output_data['project'] = project
                    f.write(json.dumps(output_data) + '\n')
    subprocess.check_call(['aws', 's3', 'cp', file, f'{s3_out}/out/ldsc/genetic_correlation/{phenotype}/'])
    os.remove(file)


def main():
    phenotype_ancestries = get_phenotype_project_ancestries()
    for phenotype, project_ancestries in phenotype_ancestries.items():
        data = {}
        for project, ancestries in project_ancestries.items():
            project_data = get_data(phenotype, project, ancestries)
            data[project] = meta_analyze(project_data)
        upload_data(phenotype, data)


if __name__ == '__main__':
    main()
