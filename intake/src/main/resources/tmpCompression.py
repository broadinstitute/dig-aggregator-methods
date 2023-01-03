#!/usr/bin/python3

import argparse
import os
import platform
import subprocess

from pyspark.sql import SparkSession

s3dir = 's3://dig-analysis-data'


# If using spark to overwrite json files will need to download them locally and copy them after
def copy_file_to_local(srcdir, file):
    subprocess.check_call(['aws', 's3', 'cp', f'{srcdir}/{file}', f'./{file}'])


def copy_file_to_s3(outdir, file):
    subprocess.check_call(['aws', 's3', 'cp', f'./{file}', f'{outdir}/{file}'])
    os.remove(f'./{file}')


def delete_file_from_s3(srcdir, file):
    subprocess.check_call(['aws', 's3', 'rm', f'{srcdir}/{file}'])


def compress_local_file(file):
    subprocess.check_call(['zstd', f'./{file}'])


def read_spark_json(srcdir):
    return spark.read.json(f'{srcdir}/part-*.json')


def write_variant_json(df, outdir):
    df.write \
        .mode('overwrite') \
        .option("ignoreNullFields", "false") \
        .option("compression", "org.apache.hadoop.io.compress.ZStandardCodec") \
        .json(outdir)


# entry point
if __name__ == '__main__':
    """
    @param method/dataset/phenotype e.g. `GWAS/Anstee2020_NAFLD_eu/NAFLD`
    """
    print('Python version: %s' % platform.python_version())

    # get method/dataset/phenotype from commandline
    opts = argparse.ArgumentParser()
    opts.add_argument('method_dataset_phenotype')
    args = opts.parse_args()

    tech, dataset, phenotype = args.method_dataset_phenotype.split('/')

    # create a spark session and dataframe from part files
    spark = SparkSession.builder.appName('compression').getOrCreate()

    # get the source and output directories (method_dataset is formatted as method/dataset here)
    srcdir = f'{s3dir}/variants_processed/{args.method_dataset_phenotype}'
    outdir = f'{s3dir}/variants_processed/{args.method_dataset_phenotype}'

    file = f'{dataset}.{phenotype}.json'
    copy_file_to_local(srcdir, file)
    compress_local_file(file)
    copy_file_to_s3(outdir, f'{file}.zst')
    delete_file_from_s3(srcdir, file)

    # done
    spark.stop()
