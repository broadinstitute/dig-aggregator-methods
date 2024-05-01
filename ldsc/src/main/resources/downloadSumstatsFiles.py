#!/usr/bin/python3
import argparse
import subprocess

downloaded_files = '/mnt/var/ldsc'
sumstats_files = f'{downloaded_files}/sumstats'


def download_sumstats(project, path):
    f_in = f'{path}/out/ldsc/sumstats/'
    f_out = f'{sumstats_files}/{project}/'
    subprocess.check_call(
        ['sudo', 'aws', 's3', 'cp', f_in, f_out, '--recursive', '--exclude="*"', '--include="*.sumstats.gz*"']
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-path', default=None, required=True, type=str)
    parser.add_argument('--project', default=None, required=True, type=str)
    args = parser.parse_args()
    s3_in = args.input_path
    project = args.project

    download_sumstats(project, s3_in)
    if project != 'portal':
        download_sumstats('portal', 's3://dig-analysis-data')


if __name__ == '__main__':
    main()
