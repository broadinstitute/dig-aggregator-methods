#!/usr/bin/python3
from argparse import ArgumentParser
import glob
import json
import os
import shutil
import subprocess

s3_in = 's3://dig-giant-sandbox'
s3_out = 's3://dig-giant-sandbox'


def download_data(args):
    src_dir = f'{s3_in}/out/credible_sets/{args.phenotype}/{args.ancestry}/'
    subprocess.check_call(['aws', 's3', 'cp', src_dir, f'./{args.phenotype}/{args.ancestry}/', '--recursive'])
    paths = set()
    with open('part-tmp.json', 'w') as f_out:
        for file in glob.glob(f'{args.phenotype}/{args.ancestry}/*/*/part-*', recursive=True):
            paths |= {'/'.join(file.split('/')[2:4])}
            with open(file, 'r') as f_in:
                shutil.copyfileobj(f_in, f_out)
        return paths


def get_out_dict():
    out_dict = {}
    adjustments = 0
    with open('part-tmp.json', 'r') as f:
        for idx, line in enumerate(f):
            line_dict = json.loads(line.strip())
            key = (line_dict['credibleSetId'], line_dict['varId'])
            value = (idx, float(line_dict['posteriorProbability']))
            if key not in out_dict or value[1] > out_dict[key][1]:
                adjustments += key in out_dict
                out_dict[key] = value
    return out_dict, adjustments


def adjust():
    out_dict, adjustments = get_out_dict()
    if adjustments == 0:
        shutil.copyfile('part-tmp.json', 'part-00000.json')
    else:
        idxs = set([idx[0] for idx in out_dict.values()])
        with open('part-00000.json', 'w') as f_out:
            with open('part-tmp.json', 'r') as f_in:
                for idx, line in enumerate(f_in):
                    if idx in idxs:
                        f_out.write(line)


def upload(args):
    out_dir = f'{s3_out}/out/credible_sets/{args.phenotype}/{args.ancestry}/merged/'
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', 'part-00000.json', out_dir])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', out_dir])
    os.remove('_SUCCESS')
    os.remove('part-tmp.json')
    os.remove('part-00000.json')
    shutil.rmtree(f'{args.phenotype}/{args.ancestry}')


def main():
    opts = ArgumentParser()
    opts.add_argument('--phenotype', type=str, required=True)
    opts.add_argument('--ancestry', type=str)
    args = opts.parse_args()

    download_data(args)
    adjust()
    upload(args)


# entry point
if __name__ == '__main__':
    main()
