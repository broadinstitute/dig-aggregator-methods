#!/usr/bin/python3
import boto3
import re
import subprocess

downloaded_files = '/mnt/var/ldsc'
sumstat_files = f'{downloaded_files}/sumstats'


def download_ancestry_sumstats():
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('dig-analysis-data')
    for file in my_bucket.objects.filter(Prefix='out/ldsc/sumstats/').all():
        if re.fullmatch(f'.*\.sumstats\.gz$', file.key):
            ancestry = re.findall(f'.*_(\w+).sumstats.gz', file.key)[0]
            subprocess.check_call([
                'sudo', 'aws', 's3', 'cp', f's3://dig-analysis-data/{file.key}', f'{sumstat_files}/{ancestry}/'
            ])


if __name__ == '__main__':
    download_ancestry_sumstats()
