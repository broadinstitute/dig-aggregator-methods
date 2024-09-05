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


def p_in(values, key):
    return [[a[key] for a in values if a['tissue'] == b['tissue']] for b in values]


def download_data(project, phenotype, ancestry):
    filename = f'{phenotype}_{ancestry}.json'
    file = f'{s3_in}/out/credible_sets/c2ct/{project}/{phenotype}/{ancestry}/part-00000.json'
    subprocess.check_call(['aws', 's3', 'cp', file, filename])
    return filename


def checkfile(filename):
    with open(filename, 'r') as f:
        return len(f.readlines()) > 0


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


p_funcs = {
    'all': p_all,
    'out': p_out,
    'in': p_in
}
def add_hq(cred_group):
    print(cred_group[0]['credibleSetId'])
    for entropy_key, p_func in p_funcs.items():
        h, p = calculate_hp(cred_group, p_func, 'posteriorProbability')
        for i in range(len(cred_group)):
            cred_group[i][f'entPP_{entropy_key}'] = p[i]
            cred_group[i][f'entropy_{entropy_key}'] = h[i]
            cred_group[i][f'totalEntropy_{entropy_key}'] = h[i] - math.log(cred_group[i]['posteriorProbability'], 2)
            cred_group[i][f'Q_{entropy_key}'] = 1 - cred_group[i][f'totalEntropy_{entropy_key}'] / 5
    return cred_group


def to_min_data(d):
    min_data = {
        'annotation': d['annotation'],
        'tissue': d['tissue'],
        'biosample': d['biosample'],
        'credibleSetId': d['credibleSetId'],
    }
    for entropy_key in p_funcs:
        min_data[f'Q_{entropy_key}'] = d[f'Q_{entropy_key}']
    return min_data


def calculate(filename, file_out):
    min_data = []
    with open(f'unfiltered.{file_out}', 'w') as f:
        for cred_groups in get_cred_groups(filename, chunk_size=cpus):
            with multiprocessing.Pool(cpus) as p:
                for cred_group in p.map(add_hq, cred_groups):
                    for d in cred_group:
                        min_data.append(to_min_data(d))
                        f.write(f'{json.dumps(d)}\n')
    return min_data


def get_q_threshold(q):
    q_sorted = sorted(q)
    min_idx = -min(1000, len(q))  # at minimum return 1000 (or all) for each filter
    min_threshold = q_sorted[min_idx]
    max_idx = -min(10000 // 3, len(q))  # at maximum return 10K across all three filters
    max_threshold = q_sorted[max_idx]
    return max(max_threshold, min(min_threshold, 0.0))  # Return all positive if min/max crosses 0.0


filters = {
    'all': lambda data: 'all',
    'annotation': lambda data: data['annotation'],
    'tissue': lambda data: (data['annotation'], data['tissue']),
    'biosample': lambda data: (data['annotation'], data['tissue'], data['biosample']),
    'credible_set_id': lambda data: data['credibleSetId']
}
def group_by(min_data, filter_fnc):
    q = {}
    for data in min_data:
        key = filter_fnc(data)
        if key not in q:
            q[key] = {entropy_key: [] for entropy_key in p_funcs}
        for entropy_key in p_funcs:
            q[key][entropy_key].append(data[f'Q_{entropy_key}'])
    return q


def get_threshold(min_data, filter_fnc):
    out = {}
    for key, qs in group_by(min_data, filter_fnc).items():
        out[key] = {}
        for entropy_key, q in qs.items():
            out[key][entropy_key] = get_q_threshold(q)
    return out


def get_thresholds(min_data):
    thresholds = {}
    for name, filter_fnc in filters.items():
        thresholds[name] = get_threshold(min_data, filter_fnc)
    return thresholds


def filter_by_q(file_out, min_data):
    all_thresholds = get_thresholds(min_data)
    with open(f'unfiltered.{file_out}', 'r') as f_in:
        f_outs = {key: open(f'{key}.{file_out}', 'w') for key in filters}
        for line in f_in:
            line_min_data = to_min_data(json.loads(line.strip()))
            for key, filter_fnc in filters.items():
                thresholds = all_thresholds[key][filter_fnc(line_min_data)]
                if any([line_min_data[f'Q_{entropy_key}'] >= thresholds[entropy_key] for entropy_key in p_funcs]):
                    f_outs[key].write(line)
        [f_out.close() for f_out in f_outs.values()]


def success(path_out):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', path_out])
    os.remove('_SUCCESS')


def upload_data(project, phenotype, ancestry, file_out):
    path_out = f'{s3_out}/out/credible_sets/specificity/{project}/{phenotype}/{ancestry}/'
    for key in filters:
        subprocess.check_call(['aws', 's3', 'cp', f'{key}.{file_out}', path_out])
        os.remove(f'{key}.{file_out}')
    subprocess.check_call(['aws', 's3', 'cp', f'unfiltered.{file_out}', path_out])
    os.remove(f'unfiltered.{file_out}')
    success(path_out)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', type=str, required=True)
    parser.add_argument('--phenotype', type=str, required=True)
    parser.add_argument('--ancestry', type=str, required=True)
    args = parser.parse_args()

    filename = download_data(args.project, args.phenotype, args.ancestry)
    if checkfile(filename):
        file_out = f'{args.phenotype}_{args.ancestry}.json'
        min_data = calculate(filename, file_out)
        filter_by_q(file_out, min_data)
        upload_data(args.project, args.phenotype, args.ancestry, file_out)
        os.remove(filename)


if __name__ == '__main__':
    main()
