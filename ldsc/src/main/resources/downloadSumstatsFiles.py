#!/usr/bin/python3
import boto3
import re
import subprocess

downloaded_files = '/mnt/var/ldsc'
sumstat_files = f'{downloaded_files}/sumstats'

buckets = ['dig-analysis-data', 'dig-analysis-hermes']

def download_ancestry_sumstats(bucket):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    for file in my_bucket.objects.filter(Prefix='out/ldsc/sumstats/').all():
        if re.fullmatch(f'.*\.sumstats\.gz$', file.key):
            ancestry = re.findall(f'.*_(\w+).sumstats.gz', file.key)[0]
            subprocess.check_call([
                'sudo', 'aws', 's3', 'cp', f's3://{bucket}/{file.key}', f'{sumstat_files}/{ancestry}/'
            ])


if __name__ == '__main__':
    for bucket in buckets:
        download_ancestry_sumstats(bucket)
