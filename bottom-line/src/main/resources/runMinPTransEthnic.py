#!/usr/bin/python3
import argparse
import re
import subprocess

from pyspark.sql import SparkSession

s3dir = 's3://dig-analysis-data'


# update the analysis and keep variants with the smallest pValue
def min_p(df):
    return df \
        .rdd \
        .keyBy(lambda v: v.varId) \
        .reduceByKey(lambda a, b: b if b.pValue < a.pValue else a) \
        .map(lambda v: v[1]) \
        .toDF()


def num_ancestries(phenotype):
    path = f'{s3dir}/out/metaanalysis/min_p/ancestry_specific/{phenotype}/'
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
    srcdir = f'{s3dir}/out/metaanalysis/min_p/ancestry_specific/{args.phenotype}/*/part-*'
    outdir = f'{s3dir}/out/metaanalysis/min_p/trans_ethnic/{args.phenotype}/'

    # create a spark session
    spark = SparkSession.builder.appName('bottom-line').getOrCreate()

    df = spark.read.json(srcdir) \
        .drop('ancestry')

    num_ancestry = num_ancestries(args.phenotype)
    df = min_p(df) if num_ancestry > 1 else df

    df.write \
        .mode('overwrite') \
        .option("compression", "org.apache.hadoop.io.compress.ZStandardCodec") \
        .json(outdir)


if __name__ == '__main__':
    main()
