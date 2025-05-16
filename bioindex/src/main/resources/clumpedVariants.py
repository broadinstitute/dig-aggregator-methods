import argparse
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    """
    Arguments:  ancestry- str indicating which ancestry to run the analysis against
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('--ancestry', type=str, required=True)
    args = opts.parse_args()

    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output locations
    common_dir = f'{s3_in}/out/varianteffect/variants/common'
    srcdir = f'{s3_in}/out/credible_sets/merged/*/{args.ancestry}/part-*'
    if args.ancestry == 'Mixed':
        outdir = f'{s3_bioindex}/associations/clump/trans-ethnic/'
    else:
        outdir = f'{s3_bioindex}/associations/clump/ancestry/{args.ancestry}'

    clumps = spark.read.json(srcdir)\
        .withColumn('ancestry', lit(args.ancestry))
    clumps = clumps \
        .withColumn('clump', clumps.credibleSetId) \
        .filter(clumps.source != 'credible_set') \
        .drop('credibleSetId')
    common = spark.read.json(f'{common_dir}/part-*') \
        .select('varId', 'dbSNP', 'consequence', 'nearest', 'minorAllele', 'maf', 'af')

    # join to get and common fields
    clumps = clumps.join(common, on='varId', how='left_outer')

    # write out all the clumped associations sorted by phenotype and clump
    clumps.orderBy(['phenotype', 'clump', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
