#!/usr/bin/python3
import argparse
import glob
import os
import shutil
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

gene_types = {
    'GENE': lambda x: f'"{x}"',
    'GENE_STAT': lambda x: bool(x),
    'WINDOW_STAT': lambda x: bool(x),
    'CHR': lambda x: f'"{x}"',
    'NEAREST_TO_LEAD': lambda x: bool(x),
    'CLUMP': lambda x: f'"{x}"' if not nullable(x) else 'null',
    'TRAIT': lambda x: f'"{x}"'
}


def nullable(x):
    return x in ['None']

def bool(x):
    return 'true' if x in ['True', 'true'] else 'false'


def download(trait):
    subprocess.check_call(['aws', 's3', 'cp', f'{s3_in}/out/falcon/staging/falcon/{trait}/', 'inputs/', '--recursive'])


def translate(trait):
    with open('outputs/falcon.genes', 'w') as f_out:
        for file in glob.glob('inputs/falcon.*.genes'):
            with open(file, 'r') as f:
                header = f.readline().strip().split('\t') + ['TRAIT']
                for line in f:
                    dict_line = dict(zip(header, line.strip().split('\t')))
                    dict_line['TRAIT'] = trait
                    f_out.write('{{{}}}\n'.format(
                        ','.join([f'"{h}":{gene_types.get(h, lambda x: x)(dict_line[h])}' for h in header])
                    ))


def upload(trait):
    subprocess.check_call(['aws', 's3', 'cp', 'outputs/falcon.genes', f'{s3_out}/out/falcon/genes/{trait}/'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--trait', default=None, required=True, type=str)
    args = parser.parse_args()

    os.makedirs('outputs', exist_ok=True)
    download(args.trait)
    translate(args.trait)
    upload(args.trait)
    shutil.rmtree('inputs')
    shutil.rmtree('outputs')


if __name__ == '__main__':
    main()
