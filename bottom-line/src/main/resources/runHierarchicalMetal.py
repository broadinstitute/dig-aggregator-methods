#!/usr/bin/python3
import argparse
import glob
import numpy as np
import os
import shutil
import subprocess
from scipy.cluster.hierarchy import linkage


downloaded_files = '/mnt/var/metal'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

LINKAGE_METHOD = 'average'


def download_gc(phenotype, ancestry):
    path_in = f'{s3_in}/out/ldsc/staging/dataset_genetic_correlation/{phenotype}/{ancestry}/'
    subprocess.check_call(['aws', 's3', 'cp', path_in, 'gc/', '--recursive'])


def parse_logs():
    # the last table of each ldsc --rg log has one row per pair: p1 p2 rg se z p h2_obs h2_obs_se h2_int h2_int_se gcov_int gcov_int_se
    pairs = {}
    for log_file in sorted(glob.glob('gc/*.log')):
        with open(log_file) as f:
            lines = f.read().split('\n')
        start = [i for i, line in enumerate(lines) if line.startswith('Summary of Genetic Correlation')][0]
        for line in lines[start + 2:]:
            fields = line.split()
            if len(fields) == 12 and fields[0].endswith('.sumstats.gz'):
                p1 = fields[0].split('/')[-2]
                p2 = fields[1].split('/')[-2]
                if p1 == p2:
                    continue
                try:
                    gcov = float(fields[10])
                except ValueError:
                    continue
                # a pair is run from both sides, keep the first estimate seen
                pairs.setdefault(frozenset([p1, p2]), gcov)

    names = sorted(set().union(*pairs))
    index = {name: i for i, name in enumerate(names)}
    gcov = np.full((len(names), len(names)), np.nan)
    for pair, value in pairs.items():
        i, j = [index[name] for name in pair]
        gcov[i, j] = gcov[j, i] = value
    return names, gcov


def write_recipe(names, gcov):
    # distance is 1 - gcov_int (clipped to 0-1), pairs with no estimate get the maximum distance of 1
    n = len(names)
    upper = gcov[np.triu_indices(n, 1)]
    tree = linkage(np.where(np.isnan(upper), 1.0, 1 - np.clip(upper, 0, 1)), method=LINKAGE_METHOD, optimal_ordering=True)

    # leaves are 0..n-1, merge idx creates node n + idx
    label = {i: name for i, name in enumerate(names)}
    with open('outputs/recipe.txt', 'w') as f:
        f.write('combined_file\tinput_1\tinput_2\tdistance\n')
        for idx, (left, right, distance, _) in enumerate(tree):
            label[n + idx] = f'combined_{idx}'
            f.write(f'{label[n + idx]}\t{label[int(left)]}\t{label[int(right)]}\t{distance:.6f}\n')
    return label[n + len(tree) - 1]


def fetch(phenotype, ancestry, input):
    if not os.path.exists(f'outputs/{input}.tsv'):
        with open(f'outputs/{input}.tsv', 'w') as f_out:
            f_out.write('MarkerName\tAllele1\tAllele2\tP-value\tZscore\tWeight\tN\n')
            path_in = f'{s3_in}/out/metaanalysis/variants/{phenotype}/dataset={input}/ancestry={ancestry}/rare=false/'
            subprocess.check_call(['aws', 's3', 'cp', path_in, 'tmp/', '--recursive'])
            for file in glob.glob('tmp/part-*.csv.zst'):
                subprocess.check_call(['zstd', '-d', '--rm', file])
                with open(os.path.splitext(file)[0], 'r') as f:
                    header = f.readline().strip().split('\t')
                    for line in f:
                        json_line = dict(zip(header, line.strip().split('\t')))
                        f_out.write('{}\t{}\t{}\t{}\t{}\t{}\t{}\n'.format(
                            json_line['varId'],
                            json_line['reference'],
                            json_line['alt'],
                            json_line['pValue'],
                            float(json_line['beta']) / float(json_line['stdErr']),
                            json_line['n'],
                            json_line['n']
                        ))
        shutil.rmtree('tmp')


def run_METAL_step(phenotype, ancestry, input_1, input_2, output):
    fetch(phenotype, ancestry, input_1)
    fetch(phenotype, ancestry, input_2)
    with open('metal.script', 'w') as f:
        f.write(
        f"""
        SCHEME SAMPLESIZE
        OVERLAP ON
        SEPARATOR TAB
        COLUMNCOUNTING LENIENT
        MARKER MarkerName
        ALLELE Allele1 Allele2
        PVAL P-value
        EFFECT Zscore
        WEIGHT Weight
        SAMPLE N
        CUSTOMVARIABLE TotalSampleSize
        LABEL TotalSampleSize AS n
        PROCESS outputs/{input_1}.tsv
        PROCESS outputs/{input_2}.tsv
        ANALYZE
        QUIT
        """.strip() + '\n')
    subprocess.check_call([f'{downloaded_files}/metal/build/metal/metal', 'metal.script'])
    os.rename('METAANALYSIS1.TBL', f'outputs/{output}.tsv')


def run_METAL(phenotype, ancestry):
    with open('outputs/recipe.txt', 'r') as f:
        _ = f.readline()
        for line in f:
            output, input_1, input_2 = line.strip().split('\t')[:3]
            run_METAL_step(phenotype, ancestry, input_1, input_2, output)


def run_METAL_metadata():
    input_files = []
    output_files = set()
    with open('outputs/recipe.txt', 'r') as f:
        _ = f.readline()
        for line in f:
            output, input_1, input_2 = line.strip().split('\t')[:3]
            input_files.append(input_1)
            input_files.append(input_2)
            output_files.add(output)
    only_input_files = [file for file in input_files if file not in output_files]

    processes = '\n'.join([f'PROCESS outputs/{file}.tsv' for file in only_input_files])
    with open('metal.script', 'w') as f:
        f.write(
            f"""
        SCHEME SAMPLESIZE
        OVERLAP ON
        SEPARATOR TAB
        COLUMNCOUNTING LENIENT
        MARKER MarkerName
        ALLELE Allele1 Allele2
        PVAL P-value
        EFFECT Zscore
        WEIGHT Weight
        SAMPLE N
        CUSTOMVARIABLE TotalSampleSize
        LABEL TotalSampleSize AS n
        {processes}
        ANALYZE HETEROGENEITY
        QUIT
        """.strip() + '\n')
    subprocess.check_call([f'{downloaded_files}/metal/build/metal/metal', 'metal.script'])
    os.rename('METAANALYSIS1.TBL', f'outputs/metadata_run.tsv')


def upload(phenotype, ancestry, final_file):
    f_out = f'{s3_out}/out/metaanalysis/bottom-line/staging/hierarchical/{phenotype}/{ancestry}'
    subprocess.check_call(['aws', 's3', 'cp', 'outputs/recipe.txt', f'{f_out}/'])
    subprocess.check_call(['aws', 's3', 'cp', f'outputs/{final_file}.tsv', f'{f_out}/'])
    subprocess.check_call(['aws', 's3', 'cp', f'outputs/metadata_run.tsv', f'{f_out}/'])
    subprocess.check_call(['touch', '_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', '_SUCCESS', f'{f_out}/'])
    os.remove('_SUCCESS')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phenotype', default=None, required=True, type=str)
    parser.add_argument('--ancestry', default=None, required=True, type=str)
    args = parser.parse_args()

    os.makedirs('inputs', exist_ok=True)
    os.makedirs('outputs', exist_ok=True)

    download_gc(args.phenotype, args.ancestry)
    names, gcov = parse_logs()
    final_file = write_recipe(names, gcov)
    run_METAL(args.phenotype, args.ancestry)
    upload(args.phenotype, args.ancestry, final_file)

    shutil.rmtree('inputs')
    shutil.rmtree('outputs')


if __name__ == '__main__':
    main()
