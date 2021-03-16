#!/usr/bin/python3
import argparse
import platform

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, lit, regexp_replace, when

S3DIR = 's3://dig-analysis-data'

# BED files need to be sorted by chrom/start, this orders the chromosomes
CHROMOSOMES = list(map(lambda c: str(c + 1), range(22))) + ['X', 'Y', 'MT']


def main():
    """
    Arguments: type/dataset
    """
    print('Python version: %s' % platform.python_version())

    opts = argparse.ArgumentParser()
    opts.add_argument('dataset')

    # extract the dataset from the command line
    args = opts.parse_args()

    # get the source and output directories
    srcdir = f'{S3DIR}/annotated_regions/cis-regulatory_elements/{args.dataset}/part-*'
    outdir = f'{S3DIR}/out/ldsc/regions/partitioned/{args.dataset}'

    # create a spark session
    spark = SparkSession.builder.appName('ldsc').getOrCreate()

    # read all the fields needed across the regions for the dataset
    df = spark.read.json(srcdir)

    # keep only regulatory elements
    df = df.filter(df.category == 'cis-regulatory elements')

    # fix any whitespace issues
    annotation = regexp_replace(df.annotation, ' ', '_')
    tissue = regexp_replace(df.tissue, ' ', '_')

    # build the partition name
    partition = concat_ws('___', annotation, tissue)

    # remove invalid chromosomes rows add a sort value and bed filename
    df = df.filter(df.chromosome.isin(CHROMOSOMES)) \
        .withColumn('partition', partition)

    # final output
    df = df.select(
        df.partition,
        df.chromosome,
        df.start,
        df.end,
    )

    # output the regions partitioned for GREGOR in BED format
    df.write \
        .mode('overwrite') \
        .partitionBy('partition') \
        .csv(outdir, sep='\t', header=False)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
