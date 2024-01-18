#!/usr/bin/python3
import argparse
import numpy as np
import re
from scipy.stats import norm
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, lit, sqrt, udf, when

s3dir = 's3://dig-analysis-data'

variants_schema = StructType(
    [
        StructField('varId', StringType(), nullable=False),
        StructField('chromosome', StringType(), nullable=False),
        StructField('position', IntegerType(), nullable=False),
        StructField('reference', StringType(), nullable=False),
        StructField('alt', StringType(), nullable=False),
        StructField('phenotype', StringType(), nullable=False),
        StructField('pValue', DoubleType(), nullable=False),
        StructField('beta', DoubleType(), nullable=False),
        StructField('stdErr', DoubleType(), nullable=False),
        StructField('n', DoubleType(), nullable=False),
    ]
)


@udf(returnType=DoubleType())
def pValue(beta, stdErr):
    return float(2 * norm.cdf(-abs(beta / stdErr)))


# update the analysis by combining beta, stdErr using n and updating pValue
def naive(df):
    df = df \
        .withColumn('weight', 1.0 / df.stdErr / df.stdErr) \
        .withColumn('weighted_beta', df.beta / df.stdErr / df.stdErr)
    df = df \
        .groupBy(df.varId, df.chromosome, df.position, df.reference, df.alt, df.phenotype, df.ancestry) \
        .agg({'weight': 'sum', 'weighted_beta': 'sum', 'n': 'sum'})
    df = df \
        .withColumn('n', df['sum(n)']) \
        .withColumn('stdErr', 1.0 / sqrt(df['sum(weight)'])) \
        .withColumn('beta', df['sum(weighted_beta)'] / df['sum(weight)'])
    df = df.withColumn('pValue', pValue(df.beta, df.stdErr)) \
        .drop('sum(weight)', 'sum(weighted_beta)', 'sum(n)')
    # The combination can push the value below the threshold to be set to 0, set to lowest float instead
    return df.withColumn('pValue', when(df.pValue == 0.0, np.nextafter(0, 1)).otherwise(df.pValue))


def num_dataset(phenotype, ancestry):
    path = f'{s3dir}/out/metaanalysis/variants/{phenotype}/'
    files = subprocess.check_output(['aws', 's3', 'ls', path, '--recursive']).decode().strip().split('\n')
    datasets = set()
    for file in files:
        m = re.match(f'.*/dataset=([^/]+)/ancestry={ancestry}/rare=.*/part-.*', file)
        if m:
            datasets |= {m.group(1)}
    return len(datasets)


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')
    opts.add_argument('ancestry')

    # parse the command line parameters
    args = opts.parse_args()

    # get the source and output directories
    srcdir = f'{s3dir}/out/metaanalysis/variants/{args.phenotype}/*/ancestry={args.ancestry}/*/part-*'
    outdir = f'{s3dir}/out/metaanalysis/naive/ancestry-specific/{args.phenotype}/ancestry={args.ancestry}/'

    # create a spark session
    spark = SparkSession.builder.appName('bottom-line').getOrCreate()

    columns = [col(field.name) for field in variants_schema]

    df = spark.read \
        .csv(
        srcdir,
        sep='\t',
        header=True,
        schema=variants_schema,
    ) \
        .select(*columns) \
        .withColumn('ancestry', lit(args.ancestry))

    num_datasets = num_dataset(args.phenotype, args.ancestry)
    df = naive(df) if num_datasets > 1 else df

    df.write \
        .mode('overwrite') \
        .option("compression", "org.apache.hadoop.io.compress.ZStandardCodec") \
        .json(outdir)


if __name__ == '__main__':
    main()
