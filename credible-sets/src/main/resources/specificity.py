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


def get_cred_groups(tissue_data):
    cred_groups = {}
    for d in tissue_data:
        key = d['credibleSetId']
        if key not in cred_groups:
            cred_groups[key] = []
        cred_groups[key].append(d)
    return cred_groups


def apply_adjustment(cred_groups):
    for key in cred_groups:
        for i in range(len(cred_groups[key])):
            cred_groups[key][i]['adjustedPP'] = cred_groups[key][i]['posteriorProbability'] / cred_groups[key][i]['annot_bp']
    return cred_groups


def add_h(cred_groups, p_func):
    for key in cred_groups:
        h, p = calculate_hp(cred_groups[key], p_func, 'posteriorProbability')
        h_adjust, p_adjust = calculate_hp(cred_groups[key], p_func, 'adjustedPP')
        for i in range(len(cred_groups[key])):
            cred_groups[key][i]['entPP'] = p[i]
            cred_groups[key][i]['adjustedEntPP'] = p_adjust[i]
            cred_groups[key][i]['entropy'] = h[i]
            cred_groups[key][i]['adjustedEntropy'] = h_adjust[i]
            cred_groups[key][i]['totalEntropy'] = h[i] - math.log(p[i], 2)
            cred_groups[key][i]['adjustedEntropy'] = h_adjust[i] - math.log(p_adjust[i], 2)
    return cred_groups


def add_q(cred_groups, entropy_key):
    for key in cred_groups:
        for i in range(len(cred_groups[key])):
            cred_groups[key][i]['Q'] = 1 - cred_groups[key][i]['totalEntropy'] / 10
            cred_groups[key][i]['Q_adj'] = 1 - cred_groups[key][i]['adjustedEntropy'] / 10
            cred_groups[key][i]['entropyType'] = entropy_key
    return cred_groups


def calculate(tissue_data, entropy_key, p_func):
    cred_groups = get_cred_groups(tissue_data)

    cred_groups = apply_adjustment(cred_groups)
    cred_groups = add_h(cred_groups, p_func)

    cred_groups = add_q(cred_groups, entropy_key)
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
            if d['Q'] > 0 or d['Q_adj'] > 0:
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
