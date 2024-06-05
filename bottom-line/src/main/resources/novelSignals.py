import glob
import json
import os
import re
import subprocess

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def get_all_variants(phenotype, ancestry):
    path_in = f'{s3_in}/out/metaanalysis/'
    if ancestry == 'TE':
        file_path = f'*/staging/clumps/analysis/{phenotype}/variants.json'
        subprocess.check_call([path_in, 'data', '--recursive', '--exclude="*"', f'--include="{file_path}"'], shell=True)
    else:
        file_path = f'*/staging/ancestry-clumps/analysis/{phenotype}/ancestry={ancestry}/variants.json'
        subprocess.check_call([path_in, 'data', '--recursive', '--exclude="*"', f'--include="{file_path}"'], shell=True)


def get_variants_dict():
    variants = {}
    for file in glob.glob('data/*/variants.json'):
        meta_type = re.findall('data/([^/]*)/.*/variant.json', file)[0]
        variants[meta_type] = []
        with open(file, 'r') as f:
            for line in f:
                variants[meta_type].append(json.loads(line.strip()))
    return variants


def get_components(variants):



