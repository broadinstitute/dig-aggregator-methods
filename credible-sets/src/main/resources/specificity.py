#!/usr/bin/python3
import argparse
import json
import math
import os
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def p_all(values, key):
    return [[a[key] for a in values] for b in values]


def p_out(values, key):
    return [[a[key] for a in values if a['tissue'] != b['tissue']] + [b[key]] for b in values]


def download_data(phenotype, ancestry):
    file = f'{s3_in}/out/credible_sets/c2ct/{phenotype}/{ancestry}/part-00000.json'
    subprocess.check_call(['aws', 's3', 'cp', file, f'{phenotype}_{ancestry}.json'])


def calculate_hp(values, p_func, key):
    p = p_func(values, key)
    z = list(map(sum, p))
    h = [sum(map(lambda x: -x * math.log(x, 2), p[i])) / z[i] + math.log(z[i], 2) for i in range(len(p))]
    p_norm = [values[i][key] / z[i] for i in range(len(z))]
    return h, p_norm


def get_cred_group_idxs(tissue_data):
    cred_groups = {}
    for idx, d in enumerate(tissue_data):
        key = d['credibleSetId']
        if key not in cred_groups:
            cred_groups[key] = []
        cred_groups[key].append(idx)
    return cred_groups


def apply_adjustment(cred_group):
    for i in range(len(cred_group)):
        cred_group[i]['adjustedPP'] = cred_group[i]['posteriorProbability'] / cred_group[i]['annot_bp']
    return cred_group


def add_hq(cred_group, p_func, entropy_key):
    h, p = calculate_hp(cred_group, p_func, 'posteriorProbability')
    h_adjust, p_adjust = calculate_hp(cred_group, p_func, 'adjustedPP')
    for i in range(len(cred_group)):
        cred_group[i]['entPP'] = p[i]
        cred_group[i]['adjustedEntPP'] = p_adjust[i]
        cred_group[i]['entropy'] = h[i]
        cred_group[i]['adjustedEntropy'] = h_adjust[i]
        cred_group[i]['totalEntropy'] = h[i] - math.log(p[i], 2)
        cred_group[i]['adjustedEntropy'] = h_adjust[i] - math.log(p_adjust[i], 2)
        cred_group[i]['Q'] = 1 - cred_group[i]['totalEntropy'] / 10
        cred_group[i]['Q_adj'] = 1 - cred_group[i]['adjustedEntropy'] / 10
        cred_group[i]['entropyType'] = entropy_key
    return cred_group


def filter_cred_group(cred_group):
    return [d for d in cred_group if d['Q'] > 0 or d['Q_adj'] > 0]


def calculate(tissue_data, entropy_key, p_func):
    cred_groups = {}
    cred_group_idxs = get_cred_group_idxs(tissue_data)
    for key, cred_group_idx in cred_group_idxs.items():
        cred_group = [tissue_data[idx] for idx in cred_group_idx]
        cred_group = apply_adjustment(cred_group)
        cred_group = add_hq(cred_group, p_func, entropy_key)
        cred_groups[key] = filter_cred_group(cred_group)
    return [value for values in cred_groups.values() for value in values]


def success(path_out):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', path_out])
    os.remove('_SUCCESS')


def upload_data(phenotype, ancestry, entropy_key, data):
    file_out = f'{phenotype}_{ancestry}_{entropy_key}.json'
    path_out = f'{s3_out}/out/credible_sets/specificity/{phenotype}/{ancestry}/{entropy_key}/'
    with open(file_out, 'w') as f:
        for d in data:
            f.write(f'{json.dumps(d)}\n')
    subprocess.check_call(['aws', 's3', 'cp', file_out, path_out])
    os.remove(file_out)
    success(path_out)


p_funcs = {
    'all': p_all,
    'out': p_out
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', type=str, required=True)
    parser.add_argument('--ancestry', type=str, required=True)
    parser.add_argument('--entropy-type', type=str, required=True)
    args = parser.parse_args()

    download_data(args.phenotype, args.ancestry)
    data = []
    with open(f'{args.phenotype}_{args.ancestry}.json', 'r') as f:
        for line in f:
            json_line = json.loads(line.strip())
            if float(json_line['posteriorProbability']) > 0.001:
                data.append(json_line)

    data_out = calculate(data, args.entropy_type, p_funcs[args.entropy_type])
    upload_data(args.phenotype, args.ancestry, args.entropy_type, data_out)


if __name__ == '__main__':
    main()
