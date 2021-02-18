from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output locations
    srcdir = f's3://dig-analysis-data/out/metaanalysis'
    outdir = f's3://dig-bio-index/associations'

    # common vep data
    common_dir = 's3://dig-analysis-data/out/varianteffect/common/part-*'

    # load the top-association, lead SNPs for every phenotype
    df = spark.read.json(f'{srcdir}/top/*/part-*')
    df = df.filter(df.leadSNP)

    # load common data for variants and join
    common = spark.read.json(common_dir)
    df = df.join(common, on='varId', how='left')

    # sort all by clump range
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
