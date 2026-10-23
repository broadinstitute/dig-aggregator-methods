#!/usr/bin/python3
import argparse
import json
import os
import subprocess

downloaded_files = '/mnt/var/pigean'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download_data(trait_group, phenotype, model):
    file_path = f'{s3_in}/out/pigean/staging/factor/{trait_group}/{phenotype}/{model}'
    subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/f.out', '.'])
    subprocess.check_call(['aws', 's3', 'cp', f'{file_path}/gc.out', '.'])


def run_graph():
    cmd = [
              'python3', f'{downloaded_files}/factor_graph.py',
              '--gene-factors-in', 'gc.out',
              '--factors-in', 'f.out',
              '--json-out', 'graph.json'
        ]
    subprocess.check_call(cmd)


def add_fields(trait_group, phenotype, model):
    file = 'pigean.graph.json'
    with open(file, 'w') as f_out:
        with open('graph.json', 'r') as f_in:
            data = json.load(f_in)
        data['trait_group'] = trait_group
        data['phenotype'] = phenotype
        data['model'] = model
        json.dump(data, f_out)
    os.remove('graph.json')
    return file


def success(file_path):
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', file_path])
    os.remove('_SUCCESS')


def upload_data(file, trait_group, phenotype, model):
    file_path = f'{s3_out}/out/pigean/graph/{model}/{trait_group}/{phenotype}/'
    subprocess.check_call(['aws', 's3', 'cp', file, file_path])
    os.remove(file)
    success(file_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--trait-group', default=None, required=True, type=str,
                        help="Input phenotype group.")
    parser.add_argument('--phenotype', default=None, required=True, type=str,
                        help="Input phenotype.")
    parser.add_argument('--model', default=None, required=True, type=str,
                        help="model (e.g. mouse_msigdb).")
    args = parser.parse_args()

    download_data(args.trait_group, args.phenotype, args.model)
    try:
        run_graph()
        file = add_fields(args.trait_group, args.phenotype, args.model)
        upload_data(file, args.trait_group, args.phenotype, args.model)
    except Exception:
        print('Error')
    os.remove('gc.out')
    os.remove('f.out')


if __name__ == '__main__':
    main()
