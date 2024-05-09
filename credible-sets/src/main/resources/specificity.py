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
    filename = f'{phenotype}_{ancestry}.json'
    file = f'{s3_in}/out/credible_sets/c2ct/{phenotype}/{ancestry}/part-00000.json'
    subprocess.check_call(['aws', 's3', 'cp', file, filename])
    return filename


def calculate_hp(values, p_func, key):
    p = p_func(values, key)
    z = list(map(sum, p))
    h = [sum(map(lambda x: -x * math.log(x, 2), p[i])) / z[i] + math.log(z[i], 2) for i in range(len(p))]
    p_norm = [values[i][key] / z[i] for i in range(len(z))]
    return h, p_norm


def get_cred_groups(filename):
    with open(filename, 'r') as f:
        json_line = json.loads(f.readline().strip())
        cred_set_id = json_line['credibleSetId']
        data = [json_line]
        for line in f:
            json_line = json.loads(line.strip())
            new_cred_id = json_line['credibleSetId']
            if new_cred_id != cred_set_id:
                yield cred_set_id, data
                data = [json_line]
                cred_set_id = new_cred_id
            else:
                data.append(json_line)
    yield cred_set_id, data


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


def calculate(filename, file_out, entropy_key, p_func):
    with open(file_out, 'w') as f:
        for cred_set_id, cred_group in get_cred_groups(filename):
            cred_group = apply_adjustment(cred_group)
            cred_group = add_hq(cred_group, p_func, entropy_key)
            cred_group = filter_cred_group(cred_group)
            for d in cred_group:
                f.write(f'{json.dumps(d)}\n')


def success(path_out):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', path_out])
    os.remove('_SUCCESS')


def upload_data(phenotype, ancestry, entropy_key, file_out):
    path_out = f'{s3_out}/out/credible_sets/specificity/{phenotype}/{ancestry}/{entropy_key}/'
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

    filename = download_data(args.phenotype, args.ancestry)
    file_out = f'{args.phenotype}_{args.ancestry}_{args.entropy_type}.json'
    calculate(filename, file_out, args.entropy_type, p_funcs[args.entropy_type])
    upload_data(args.phenotype, args.ancestry, args.entropy_type, file_out)


if __name__ == '__main__':
    main()
