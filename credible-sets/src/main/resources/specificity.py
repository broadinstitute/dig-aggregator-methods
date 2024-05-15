#!/usr/bin/python3
import argparse
import json
import math
import multiprocessing
import os
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

cpus = 8


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


def get_cred_groups(filename, chunk_size):
    with open(filename, 'r') as f:
        chunk = []
        json_line = json.loads(f.readline().strip())
        cred_set_id = json_line['credibleSetId']
        data = [json_line]
        for line in f:
            json_line = json.loads(line.strip())
            new_cred_id = json_line['credibleSetId']
            if new_cred_id != cred_set_id:
                chunk.append(data)
                if len(chunk) == chunk_size:
                    yield chunk
                    chunk = []
                data = [json_line]
                cred_set_id = new_cred_id
            else:
                data.append(json_line)
    chunk.append(data)
    yield chunk


def add_hq(args):
    entropy_key, cred_group = args
    print(cred_group[0]['credibleSetId'])
    h, p = calculate_hp(cred_group, p_funcs[entropy_key], 'posteriorProbability')
    for i in range(len(cred_group)):
        cred_group[i]['entPP'] = p[i]
        cred_group[i]['entropy'] = h[i]
        cred_group[i]['totalEntropy'] = h[i] - math.log(cred_group[i]['posteriorProbability'], 2)
        cred_group[i]['Q'] = 1 - cred_group[i]['totalEntropy'] / 5
        cred_group[i]['entropyType'] = entropy_key
    return cred_group


def calculate(filename, file_out, entropy_key):
    with open(file_out, 'w') as f:
        for cred_groups in get_cred_groups(filename, chunk_size=cpus):
            with multiprocessing.Pool(cpus) as p:
                for cred_group in p.map(add_hq, [(entropy_key, cred_group) for cred_group in cred_groups]):
                    for d in cred_group:
                        if d['Q'] > 0:
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
    calculate(filename, file_out, args.entropy_type)
    upload_data(args.phenotype, args.ancestry, args.entropy_type, file_out)


if __name__ == '__main__':
    main()
