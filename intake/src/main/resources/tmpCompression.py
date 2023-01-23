#!/usr/bin/python3

import argparse
import os
import platform
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType, DoubleType


input_s3dir = 's3://dig-analysis-data'
tmp_s3dir = 's3://psmadbec-test'

variants_schema = StructType([
    StructField('varId', StringType(), nullable=False),
    StructField('chromosome', StringType(), nullable=False),
    StructField('position', IntegerType(), nullable=False),
    StructField('reference', StringType(), nullable=False),
    StructField('alt', StringType(), nullable=False),
    StructField('multiAllelic', BooleanType(), nullable=False),
    StructField('dataset', StringType(), nullable=False),
    StructField('phenotype', StringType(), nullable=False),
    StructField('ancestry', StringType(), nullable=True),
    StructField('pValue', DoubleType(), nullable=False),
    StructField('beta', FloatType(), nullable=True),
    StructField('oddsRatio', DoubleType(), nullable=True),
    StructField('eaf', FloatType(), nullable=True),
    StructField('maf', FloatType(), nullable=True),
    StructField('stdErr', FloatType(), nullable=True),
    StructField('zScore', FloatType(), nullable=True),
    StructField('n', FloatType(), nullable=True),
    StructField('filter_reason', StringType(), nullable=True)
])


# If using spark to overwrite json files will need to download them locally and copy them after
def copy_file_to_local(srcdir, file):
    subprocess.check_call(['aws', 's3', 'cp', f'{srcdir}/{file}', f'./{file}'])


def copy_file_to_s3(outdir, file):
    subprocess.check_call(['aws', 's3', 'cp', f'./{file}', f'{outdir}/{file}'])
    os.remove(f'./{file}')


def compress_local_file(file):
    subprocess.check_call(['zstd', f'./{file}'])


def read_spark_json(srcdir):
    return spark.read.json(f'{srcdir}/part-*')


def read_spark_json_with_schema(srcdir):
    return spark.read.json(f'{srcdir}/part-*', schema=variants_schema)


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

    # variants_processed
    srcdir = f'{input_s3dir}/variants_processed/{args.method_dataset_phenotype}'
    outdir = f'{tmp_s3dir}/variants_processed/{args.method_dataset_phenotype}'

    file = f'{dataset}.{phenotype}.json'
    log_file = f'{dataset}.{phenotype}.log'
    copy_file_to_local(srcdir, file)
    copy_file_to_local(srcdir, log_file)
    copy_file_to_local(srcdir, 'metadata')
    compress_local_file(file)
    copy_file_to_s3(outdir, f'{file}.zst')
    copy_file_to_s3(outdir, log_file)
    copy_file_to_s3(outdir, 'metadata')
    os.remove(f'./{file}')

    # variants_qc pass
    srcdir = f'{input_s3dir}/variants_qc/{args.method_dataset_phenotype}/pass'
    outdir = f'{tmp_s3dir}/variants_qc/{args.method_dataset_phenotype}/pass'

    df = read_spark_json(srcdir)
    copy_file_to_local(srcdir, 'metadata')
    write_variant_json(df, outdir)
    copy_file_to_s3(outdir, 'metadata')

    # variants_qc fail
    srcdir = f'{input_s3dir}/variants_qc/{args.method_dataset_phenotype}/fail'
    outdir = f'{tmp_s3dir}/variants_qc/{args.method_dataset_phenotype}/fail'

    df = read_spark_json_with_schema(srcdir)
    write_variant_json(df, outdir)

    # variants
    srcdir = f'{input_s3dir}/variants/{args.method_dataset_phenotype}'
    outdir = f'{tmp_s3dir}/variants/{args.method_dataset_phenotype}'

    df = read_spark_json(srcdir)
    copy_file_to_local(srcdir, 'scaling.log')
    write_variant_json(df, outdir)
    copy_file_to_s3(outdir, 'scaling.log')

    # done
    spark.stop()
