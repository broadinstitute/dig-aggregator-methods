#!/usr/bin/python3
import argparse
import re
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import lit, col

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


# update the analysis and keep variants with the smallest pValue
def min_p(df):
    return df \
        .rdd \
        .keyBy(lambda v: v.varId) \
        .reduceByKey(lambda a, b: b if b.pValue < a.pValue else a) \
        .map(lambda v: v[1]) \
        .toDF()


def num_dataset(phenotype, ancestry):
    path = f'{s3dir}/out/metaanalysis/variants/{phenotype}/'
    files = subprocess.check_output(['aws', 's3', 'ls', path, '--recursive']).decode().strip().split('\n')
    datasets = set()
    for file in files:
        m = re.match(f'.*/dataset=([^/]+)/ancestry={ancestry}/rare=.*/part-00000.*', file)
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
    outdir = f'{s3dir}/out/metaanalysis/min_p/ancestry_specific/{args.phenotype}/ancestry={args.ancestry}/'

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
    df = min_p(df) if num_datasets > 1 else df

    df.write \
        .mode('overwrite') \
        .option("compression", "org.apache.hadoop.io.compress.ZStandardCodec") \
        .json(outdir)


if __name__ == '__main__':
    main()
