import argparse

from pyspark.sql import SparkSession, DataFrame


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load and output directory
    srcdir = 's3://dig-analysis-data/out/metaanalysis/clumped'
    common_dir = 's3://dig-analysis-data/out/varianteffect/common'
    clump_outdir = 's3://dig-bio-index/associations/clump'
    variant_outdir = 's3://dig-bio-index/associations/variantclumps'

    # load the top association clumps
    clumps = spark.read.json(f'{srcdir}/*/part-*')
    common = spark.read.json(f'{common_dir}/part-*')

    # join to get and common fields
    clumps = clumps.join(common, on='varId', how='left_outer')

    # write out all the clumped associations sorted by phenotype and clump
    clumps.orderBy(['phenotype', 'clump', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(clump_outdir)

    # write out all the clumped associations sorted by varId
    clumps.orderBy(['varId', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(variant_outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
