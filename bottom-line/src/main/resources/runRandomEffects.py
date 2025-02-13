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


def get_af(ancestry, var_ids):
    data = {}
    with open(f'{downloaded_data}/var_to_af_{ANCESTRY_MAP[ancestry]}.tsv', 'r') as f:
        for line in f:
            var_id, af = line.strip().split('\t')
            if var_id in var_ids:
                data[var_id] = float(af)
    return data


def get_var_ids(basename):
    var_ids = set()
    with open(f'{basename}.json', 'r') as f_in:
        for line in f_in:
            json_data = json.loads(line.strip())
            var_ids |= {json_data['varId']}
    return var_ids


def process_file(file):
    basename = re.search('(.*).json.zst', file).group(1)
    ancestry = re.search('/ancestry=([^/]*)/', file).group(1)
    subprocess.check_call(f'zstd -d --rm {file}', shell=True)
    var_ids = get_var_ids(basename)
    var_to_af = get_af(ancestry, var_ids)
    with open(f'{basename}.json', 'r') as f_in:
        with open(f'{basename}.tsv', 'w') as f_out:
            for line in f_in:
                json_data = json.loads(line.strip())
                f_out.write('{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n'.format(
                    json_data['varId'],
                    json_data['chromosome'],
                    json_data['reference'],
                    json_data['alt'],
                    json_data['position'],
                    json_data['beta'],
                    json_data['stdErr'],
                    var_to_af.get(json_data['varId'], 0.0),
                    json_data['n']
                ))
    os.remove(os.path.splitext(file)[0])


def process_files(ancestry):
    files = glob.glob(f'./tmp_files/ancestry={ancestry}/part-*.zst')
    with Pool(CPUS) as p:
        p.map(process_file, files)


def combine_files(ancestry):
    with open(f'./tmp_files/MRMEGA.{ancestry}.in', 'w') as f_out:
        f_out.write('varId\tchromosome\treference\talt\tposition\tbeta\tstdErr\teaf\tn\n')
        for file in glob.glob(f'./tmp_files/ancestry={ancestry}/part-*.tsv'):
            with open(file, 'r') as f_in:
                shutil.copyfileobj(f_in, f_out)
    with open('./tmp_files/MRMEGA.in', 'a') as f:
        f.write(f'{os.path.abspath(f"./tmp_files/MRMEGA.{ancestry}.in")}\n')
    shutil.rmtree(f'./tmp_files/ancestry={ancestry}')


def run_mrmega():
    subprocess.check_call('./runMRMEGA.sh ./tmp_files', shell=True)


def upload_data(phenotype):
    path_out = f'{s3_out}/out/metaanalysis/bottom-line/staging/random-effects/{phenotype}/'
    subprocess.check_call('zstd --rm ./tmp_files/MRMEGA.tbl.result', shell=True)
    subprocess.check_call(f'aws s3 cp ./tmp_files/MRMEGA.tbl.result.zst {path_out}', shell=True)
    subprocess.check_call('touch ./tmp_files/_SUCCESS', shell=True)
    subprocess.check_call(f'aws s3 cp ./tmp_files/_SUCCESS {path_out}', shell=True)
    shutil.rmtree('./tmp_files')


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--phenotype')
    args = opts.parse_args()

    ancestries = download_data(args.phenotype)
    for ancestry in ancestries:
        process_files(ancestry)
        combine_files(ancestry)
    run_mrmega()
    upload_data(args.phenotype)


if __name__ == '__main__':
    main()
