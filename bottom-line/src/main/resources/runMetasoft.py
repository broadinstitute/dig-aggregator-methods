#!/usr/bin/python3
import argparse
import glob
import json
import shutil
from multiprocessing import Pool
import os
import re
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

CPUS = 8
ANCESTRY_MAP = {
    'AA': 'AFR',
    'AF': 'AFR',
    'SSAF': 'AFR',
    'HS': 'AMR',
    'EA': 'EAS',
    'EU': 'EUR',
    'SA': 'SAS',
    'GME': 'SAS',
    'Mixed': 'EUR'
}
downloaded_data = '/mnt/var/metaanalysis'


def download_data(phenotype):
    subprocess.check_call(f'aws s3 cp {s3_in}/out/metaanalysis/bottom-line/ancestry-specific/{phenotype}/ ./tmp_files/ --recursive', shell=True)
    return [file.split('=')[1] for file in glob.glob('tmp_files/*')]


def process_file(file):
    basename = re.search('(.*).json.zst', file).group(1)
    subprocess.check_call(f'zstd -d --rm {file}', shell=True)
    with open(f'{basename}.json', 'r') as f_in:
        with open(f'{basename}.tsv', 'w') as f_out:
            for line in f_in:
                json_data = json.loads(line.strip())
                f_out.write('{}\t{}\t{}\n'.format(
                    json_data['varId'],
                    json_data['beta'],
                    json_data['stdErr'],
                ))
    os.remove(os.path.splitext(file)[0])


def process_files(ancestry):
    files = glob.glob(f'./tmp_files/ancestry={ancestry}/part-*.zst')
    with Pool(CPUS) as p:
        p.map(process_file, files)


def get_var_map(ancestry):
    var_map = {}
    for file in glob.glob(f'./tmp_files/ancestry={ancestry}/part-*.tsv'):
        with open(file, 'r') as f:
            for line in f:
                varId, beta, stdErr = line.strip().split('\t')
                if float(stdErr) > 0.0:
                    var_map[varId] = (beta, stdErr)
        os.remove(file)
    return var_map


def valid_line(var_maps, varId, ancestries):
    beta_stdErrs = [var_maps[ancestry][varId] for ancestry in ancestries if varId in var_maps[ancestry]]
    if len(beta_stdErrs) == 2:
        beta_1, stdErr_1 = map(float, beta_stdErrs[0])
        beta_2, stdErr_2 = map(float, beta_stdErrs[1])
        beta_diff = abs(beta_1 - beta_2) / 2
        if beta_diff == stdErr_1 and beta_diff == stdErr_2:
            return False
    return True


def make_input(var_maps):
    all_keys = set()
    for var_map in var_maps.values():
        all_keys |= var_map.keys()
    ancestries = list(var_maps.keys())
    idx = 0
    count = 0
    f = open(f'./tmp_files/Metasoft_{idx}.in', 'w')
    for varId in all_keys:
        count += 1
        values = '\t'.join(['{}\t{}'.format(*var_maps[ancestry].get(varId, ('NA', 'NA'))) for ancestry in ancestries])
        if valid_line(var_maps, varId, ancestries):
            f.write(f'{varId}\t{values}\n')
            if count % 1000000 == 0:
                idx += 1
                f.close()
                f = open(f'./tmp_files/Metasoft_{idx}.in', 'w')
        else:
            print('Skipping line: ', f'{varId}\t{values}')
    f.close()


def process_input(ancestries):
    var_map = {}
    for ancestry in ancestries:
        process_files(ancestry)
        var_map[ancestry] = get_var_map(ancestry)
    make_input(var_map)


def run_metasoft(file):
    base, ext = os.path.splitext(file)
    subprocess.check_call(f'java -jar {downloaded_data}/Metasoft/Metasoft.jar '
                          f'-pvalue_table {downloaded_data}/Metasoft/HanEskinPvalueTable.txt '
                          f'-input {file}  '
                          f'-output {base}.tbl', shell=True)
    os.remove(file)


def run_metasoft_parallel():
    with Pool(CPUS) as p:
        p.map(run_metasoft, glob.glob(f'./tmp_files/Metasoft_*.in'))


def combine():
    with open('./tmp_files/Metasoft.tbl', 'w') as f_out:
        with open('./tmp_files/Metasoft_0.tbl', 'r') as f_in:
            f_out.write(f_in.readline())
        for file in glob.glob(f'./tmp_files/Metasoft_*.tbl'):
            with open(file, 'r') as f:
                _ = f.readline() # header
                for line in f:
                    f_out.write(line)


def upload_data(phenotype):
    path_out = f'{s3_out}/out/metaanalysis/bottom-line/staging/metasoft/{phenotype}/'
    subprocess.check_call('zstd --rm ./tmp_files/Metasoft.tbl', shell=True)
    subprocess.check_call(f'aws s3 cp ./tmp_files/Metasoft.tbl.zst {path_out}', shell=True)
    subprocess.check_call('touch ./tmp_files/_SUCCESS', shell=True)
    subprocess.check_call(f'aws s3 cp ./tmp_files/_SUCCESS {path_out}', shell=True)


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--phenotype')
    args = opts.parse_args()

    ancestries = download_data(args.phenotype)
    if len(ancestries) > 1:
        process_input(ancestries)
        run_metasoft_parallel()
        combine()
        upload_data(args.phenotype)
    shutil.rmtree('./tmp_files')


if __name__ == '__main__':
    main()
