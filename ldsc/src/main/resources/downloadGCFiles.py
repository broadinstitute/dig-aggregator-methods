#!/usr/bin/python3
import argparse
import subprocess

downloaded_files = '/mnt/var/ldsc'


def download_genetic_correlations(path):
    f_in = f'{path}/out/ldsc/staging/genetic_correlation/'
    subprocess.check_call(
        f'sudo aws s3 cp {f_in} {downloaded_files} --recursive --exclude="*" --include="*.log"',
        shell=True
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-path', default=None, required=True, type=str)
    args = parser.parse_args()
    s3_in = args.input_path

    download_genetic_correlations(s3_in)


if __name__ == '__main__':
    main()
