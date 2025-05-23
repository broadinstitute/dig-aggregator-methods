#!/usr/bin/python3
import argparse
import json
import os
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def check_file(file, file_path):
    return subprocess.call(f'aws s3 ls {file_path}/{file}', shell=True)


def download_file(file, file_path):
    if check_file(file, file_path) == 0:
        subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/{file}', file])


def get_label_dict(label_file, combine_key):
    out = {}
    with open(label_file, 'r') as f:
        for line in f:
            line_dict = json.loads(line.strip())
            if line_dict[combine_key] not in out:
                out[line_dict[combine_key]] = (line_dict['label'], line_dict['label_factor'])
    return out


def combine(label_file, data_file, out_file, combine_key):
    errors = 0
    label_dict = get_label_dict(label_file, combine_key)
    with open(out_file, 'w') as f_out:
        with open(data_file, 'r') as f_in:
            for line in f_in:
                try:
                    json_line = json.loads(line.strip())
                    label_factor = label_dict.get(json_line[combine_key])
                    if label_factor is not None:
                        json_line['label'] = label_factor[0]
                        json_line['factor'] = label_factor[1]
                    f_out.write(f'{json.dumps(json_line)}\n')
                except:
                    print('line invalid')
                    errors += 1
    print(f'Total Errors: {errors}')


def upload(data_file, out_file, out_path):
    subprocess.check_call(['aws', 's3', 'cp', data_file, f'{out_path}/{out_file}'])
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', f'{out_path}/_SUCCESS'])
    os.remove('_SUCCESS')


def run(trait_group, phenotype, sigma, gene_set_size, data_name, label_name, out_name, combine_key):
    label_file = f'{label_name}.json'
    data_file = f'{data_name}.json'
    out_file = f'{out_name}.json'

    label_path = f'{s3_in}/out/pigean/{label_name}/sigma={sigma}/size={gene_set_size}/{trait_group}/{phenotype}'
    data_path = f'{s3_in}/out/pigean/{data_name}/sigma={sigma}/size={gene_set_size}/{trait_group}/{phenotype}'
    out_path = f'{s3_out}/out/pigean/{out_name}/sigma={sigma}/size={gene_set_size}/{trait_group}/{phenotype}'

    download_file(label_file, label_path)
    download_file(data_file, data_path)
    if os.path.exists(label_file):
        combine(label_file, data_file, out_file, combine_key)
        upload(out_file, out_file, out_path)
        os.remove(label_file)
        os.remove(out_file)
    else:
        upload(data_file, out_file, out_path)
    os.remove(data_file)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--trait-group', default=None, required=True, type=str,
                        help="Input phenotype group.")
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    parser.add_argument('--sigma', default=None, required=True, type=str,
                        help="Sigma power (0, 2, 4).")
    parser.add_argument('--gene-set-size', default=None, required=True, type=str,
                        help="gene-set-size (small, medium, or large).")
    args = parser.parse_args()

    run(args.trait_group, args.phenotype, args.sigma, args.gene_set_size, 'gene_stats', 'gene_factor', 'combined_gene_stats', 'gene')
    run(args.trait_group, args.phenotype, args.sigma, args.gene_set_size, 'gene_set_stats', 'gene_set_factor', 'combined_gene_set_stats', 'gene_set')


if __name__ == '__main__':
    main()
