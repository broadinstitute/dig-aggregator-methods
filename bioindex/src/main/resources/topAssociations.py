import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    """
    Arguments:  ancestry - str indicating which ancestry to run the analysis against
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('--ancestry', type=str, required=True)
    args = opts.parse_args()

    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output locations
    common_dir = f'{s3_in}/out/varianteffect/common/part-*'
    srcdir = f'{s3_in}/out/credible_sets/merged/*/{args.ancestry}/part-*'
    if args.ancestry == 'Mixed':
        outdir = f'{s3_bioindex}/associations/{{}}'
    else:
        outdir = f'{s3_bioindex}/ancestry-associations/{{}}/{args.ancestry}'

    df = spark.read.json(srcdir) \
        .withColumn('ancestry', lit(args.ancestry))
    df = df \
        .withColumn('clump', df.credibleSetId) \
        .filter(df.source != 'credible_set') \
        .drop('credibleSetId')

    # load the top-association, lead SNPs for every phenotype
    df = df.filter(df.leadSNP)

    # load common data for variants and join
    common = spark.read.json(common_dir) \
        .select('varId', 'dbSNP', 'consequence', 'nearest')
    df = df.join(common, on='varId', how='left')

    # sort all by clump range
    df.orderBy(['chromosome', 'clumpStart']) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('top'))

    # sort by phenotype then p-value for global associations
    df.orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('global'))

    # done
    spark.stop()


if __name__ == '__main__':
    main()
