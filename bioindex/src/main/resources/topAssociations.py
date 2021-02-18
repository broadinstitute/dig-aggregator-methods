from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output locations
    srcdir = f's3://dig-analysis-data/out/metaanalysis/top/*/part-*'
    outdir = f's3://dig-bio-index/associations'

    # common vep data
    common_dir = 's3://dig-analysis-data/out/varianteffect/common'

    # load the top associations for each phenotype
    df = spark.read.json(srcdir)

    # drop the duplicate columns from common
    common = spark.read.json(f'{common_dir}/part-*') \
        .drop('dbSNP', 'chromosome', 'position')

    # join with common VEP data
    df = df.join(common, on='varId', how='left_outer')

    # sort all top associations together by position
    df.orderBy(['chromosome', 'clumpStart']) \
        .write \
        .mode('overwrite') \
        .json('%s/top' % outdir)

    # sort by phenotype then p-value for global associations
    df.orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json('%s/global' % outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
