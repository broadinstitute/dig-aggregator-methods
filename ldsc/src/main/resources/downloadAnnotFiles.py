#!/usr/bin/python3
import argparse
import subprocess

downloaded_files = '/mnt/var/ldsc'
annot_files = f'{downloaded_files}/annot'


def download_annot(project, path):
    f_in = f'{path}/out/ldsc/regions/combined_ld'
    f_out = f'{annot_files}/{project}/'
    subprocess.check_call(f'sudo aws s3 cp {f_in} {f_out} --recursive --exclude="*_SUCCESS"', shell=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-path', default=None, required=True, type=str)
    parser.add_argument('--project', default=None, required=True, type=str)
    args = parser.parse_args()
    s3_in = args.input_path
    project = args.project

    download_annot(project, s3_in)
    if project != 'portal':
        download_annot('portal', 's3://dig-analysis-data')


if __name__ == '__main__':
    main()
