#!/usr/bin/python3
import argparse
import glob
import json
from multiprocessing import Pool
import os
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
    return [file.split('=')[0] for file in glob.glob('temp_files/*')]


curr_var_to_af = {}
def load_var_to_af(ancestry):
    with open(f'{downloaded_data}/var_to_af/var_to_af_{ANCESTRY_MAP[ancestry]}.json', 'r') as f:
        curr_var_to_af = json.load(f)

def var_to_af(varId):
    return curr_var_to_af.get(varId, 0.0)


def process_file(file):
    subprocess.check_call(f'zstd -d --rm {file}')
    with open(os.path.splitext(file)[0], 'r') as f:
        f.write('varId\tchromosome\treference\talt\tposition\tbeta\tstdErr\teaf\tn\n')
        for line in f:
            json_data = json.loads(line.strip())
            f.write('{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n'.format(
                json_data['varId'],
                json_data['chromosome'],
                json_data['reference'],
                json_data['alt'],
                json_data['position'],
                json_data['beta'],
                json_data['stdErr'],
                var_to_af(json_data['varId']),
                json_data['n']
            ))


def process_files(ancestry):
    files = glob.glob(f'./tmp_files/ancestry={ancestry}/part-*.json')
    load_var_to_af(ancestry)
    with Pool(CPUS) as p:
        p.map(process_file, files)


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--phenotype')
    args = opts.parse_args()

    ancestries = download_data(args.phenotype)
    for ancestry in ancestries:
        process_files(ancestry)


if __name__ == '__main__':
    main()
