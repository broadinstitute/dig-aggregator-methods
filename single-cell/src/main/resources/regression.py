#!/usr/bin/python3
import argparse
import glob
import gzip
import os
import shutil
import subprocess
import zipfile

downloaded_files = '/mnt/var/pigean'
s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']

DONOR_ID = 'donor_id'


def download_files(dataset, cell_type, model):
    files = [
        f'{s3_in}/out/single_cell/staging/factor_matrix/{dataset}/{cell_type}/{model}/factor_matrix_cell_loadings.tsv',
        f'{s3_in}/out/single_cell/staging/downsample/{dataset}/{cell_type}/sample_metadata.sample.tsv.gz'
    ]
    for file in files:
        subprocess.check_call(['aws', 's3', 'cp', f'{file}', 'input/'])


def fetch_metadata():
    with gzip.open('input/sample_metadata.sample.tsv.gz', 'rt') as f:
        header = f.readline().strip().split('\t')
        possible_label_dict = {}
        for idx, label in enumerate(header):
            if label.lower() not in map(str.lower, possible_label_dict):
                possible_label_dict[label] = idx
        label_set = {label: set() for label in possible_label_dict}
        for line in f:
            split_line = [a.strip() for a in line.split('\t')]  # can have empty cells at the end of the line
            for label in possible_label_dict:
                label_value = split_line[possible_label_dict[label]]
                if label_value not in label_set[label]:
                    label_set[label] |= {label_value}
    return label_set


def get_is_float(example):
    try:
        _ = float(example)
        return True
    except:
        return False


def get_column_labels(label_set, total_donors):
    labels = []
    for label in list(label_set.keys()):
        is_float = get_is_float(next(iter(label_set[label])))
        # Only want float columns which aren't boolean (2) but are plausibly donor-level data (< total donors)
        if is_float and 2 < len(label_set[label]) <= total_donors:
            labels.append(label)
    return labels


def run_regressions(labels):
    for label_idx, label in enumerate(labels):
        subprocess.check_call(['python', f'{downloaded_files}/donor_factor_regression.py',
                              '--loadings', 'input/factor_matrix_cell_loadings.tsv',
                              '--metadata', 'input/sample_metadata.sample.tsv.gz',
                              '--donor-col', f'{DONOR_ID}',
                              '--phenotype-col', f'{label}',
                              '--collapse', 'mean',
                              '--min-cells', '1',
                              '--out', f'staging/donor_factor.{label_idx}.regression'])


def combine_regressions(labels):
    with open('output/donor_factor.regression.results.tsv', 'w') as f_out:
        f_out.write('factor\tcolumn\tcoef\tp\n')
        for idx in range(len(labels)):
            with open(f'staging/donor_factor.{idx}.regression.results.tsv', 'r') as f_in:
                header = f_in.readline().strip().split('\t')
                for line in f_in:
                    line_dict = dict(zip(header, line.strip().split('\t')))
                    f_out.write(f'{line_dict["factor"]}\t{labels[idx]}\t{line_dict["coef"]}\t{line_dict["p"]}\n')


def upload(dataset, cell_type, model):
    staging_output = f'{s3_out}/out/single_cell/staging/regression/{dataset}/{cell_type}/{model}'
    with zipfile.ZipFile('donor_factor.regression.results.zip', 'w', zipfile.ZIP_DEFLATED) as z:
        for file in glob.glob('staging/*.tsv'):
            z.write(file)
    subprocess.check_call(['aws', 's3', 'cp', 'donor_factor.regression.results.zip', f'{staging_output}/'])

    output = f'{s3_out}/out/single_cell/regression/{dataset}/{cell_type}/{model}'
    subprocess.check_call(['aws', 's3', 'cp', 'output/donor_factor.regression.results.tsv', f'{output}/'])


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--dataset', type=str, required=True)
    opts.add_argument('--cell-type', type=str, required=True)
    opts.add_argument('--model', type=str, required=True)
    args = opts.parse_args()

    download_files(args.dataset, args.cell_type, args.model)

    label_set = fetch_metadata()
    labels = get_column_labels(label_set, len(label_set[DONOR_ID]))
    if len(labels) > 0:
        os.makedirs('staging', exist_ok=True)
        run_regressions(labels)

        os.makedirs('output', exist_ok=True)
        combine_regressions(labels)

        upload(args.dataset, args.cell_type, args.model)
        shutil.rmtree('staging')
        shutil.rmtree('output')
        os.remove('donor_factor.regression.results.zip')
    shutil.rmtree('input')


if __name__ == '__main__':
    main()
