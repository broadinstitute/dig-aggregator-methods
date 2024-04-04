#!/usr/bin/python3
import argparse
import numpy as np
import os
import re
from scipy.stats import norm
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import sqrt, udf, when

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


@udf(returnType=DoubleType())
def pValue(beta, stdErr):
    return float(2 * norm.cdf(-abs(beta / stdErr)))


# update the analysis by combining beta, stdErr using n and updating pValue
def naive(df):
    df = df \
        .withColumn('weight', 1.0 / df.stdErr / df.stdErr) \
        .withColumn('weighted_beta', df.beta / df.stdErr / df.stdErr)
    df = df \
        .groupBy(df.varId, df.chromosome, df.position, df.reference, df.alt, df.phenotype) \
        .agg({'weight': 'sum', 'weighted_beta': 'sum', 'n': 'sum'})
    df = df \
        .withColumn('n', df['sum(n)']) \
        .withColumn('stdErr', 1.0 / sqrt(df['sum(weight)'])) \
        .withColumn('beta', df['sum(weighted_beta)'] / df['sum(weight)'])
    df = df.withColumn('pValue', pValue(df.beta, df.stdErr)) \
        .drop('sum(weight)', 'sum(weighted_beta)', 'sum(n)')
    # The combination can push the value below the threshold to be set to 0, set to lowest float instead
    return df.withColumn('pValue', when(df.pValue == 0.0, np.nextafter(0, 1)).otherwise(df.pValue))


def num_ancestries(phenotype):
    path = f'{s3_in}/out/metaanalysis/naive/ancestry-specific/{phenotype}/'
    files = subprocess.check_output(['aws', 's3', 'ls', path, '--recursive']).decode().strip().split('\n')
    ancestries = set()
    for file in files:
        m = re.match(f'.*/ancestry=([^/]+)/part-.*', file)
        if m:
            ancestries |= {m.group(1)}
    return len(ancestries)


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')

    # parse the command line parameters
    args = opts.parse_args()

    # get the source and output directories
    srcdir = f'{s3_in}/out/metaanalysis/naive/ancestry-specific/{args.phenotype}/*/part-*'
    outdir = f'{s3_out}/out/metaanalysis/naive/trans-ethnic/{args.phenotype}/'

    # create a spark session
    spark = SparkSession.builder.appName('bottom-line').getOrCreate()

    df = spark.read.json(srcdir) \
        .drop('ancestry')

    num_ancestry = num_ancestries(args.phenotype)
    df = naive(df) if num_ancestry > 1 else df

    df.write \
        .mode('overwrite') \
        .option("compression", "org.apache.hadoop.io.compress.ZStandardCodec") \
        .json(outdir)


if __name__ == '__main__':
    main()
