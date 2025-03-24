#!/usr/bin/python3
import argparse
import os
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def download(phenotype):
    path = f'{s3_in}/out/metaanalysis/bottom-line/staging/metasoft'
    subprocess.check_call(f'aws s3 cp {path}/{phenotype}/Metasoft.tbl.zst .', shell=True)
    subprocess.check_call('zstd -d --rm Metasoft.tbl.zst', shell=True)


def process():
    with open('part-00000.json', 'w') as f_out:
        with open('Metasoft.tbl', 'r') as f:
            header = f.readline().strip().split('\t')
            for line in f:
                line_dict = dict(zip(header, line.strip().split('\t')))
                if line_dict["PVALUE_RE2"] not in ['NA', 'NAN']:
                    f_out.write(f'{{'
                                f'"varId": "{line_dict["RSID"]}", '
                                f'"pValue": {float(line_dict["PVALUE_RE2"])}}}\n'
                                )
    subprocess.check_call('zstd --rm part-00000.json', shell=True)
    os.remove('Metasoft.tbl')


def upload(phenotype):
    path = f'{s3_out}/out/metaanalysis/metasoft/trans-ethnic/{phenotype}/'
    subprocess.check_call(f'aws s3 cp part-00000.json.zst {path}', shell=True)
    subprocess.check_call('touch _SUCCESS', shell=True)
    subprocess.check_call(f'aws s3 cp _SUCCESS {path}', shell=True)
    os.remove('part-00000.json.zst')
    os.remove('_SUCCESS')


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--phenotype')
    args = opts.parse_args()

    download(args.phenotype)
    process()
    upload(args.phenotype)


if __name__ == '__main__':
    main()
