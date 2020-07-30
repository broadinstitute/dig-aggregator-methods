from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output locations
    srcdir = f's3://dig-analysis-data/out/metaanalysis/trans-ethnic/*/part-*'
    outdir = f's3://dig-bio-index/associations'

    # common vep data
    common_dir = 's3://dig-analysis-data/out/varianteffect/common'

    # load the trans-ethnic, meta-analysis, top variants and write them sorted
    df = spark.read.json(srcdir)
    common = spark.read.json(common_dir)

    # write associations sorted by variant id
    df.join(common, 'varId', how='left_outer') \
        .orderBy(['varId']) \
        .write \
        .mode('overwrite') \
        .json('%s/variant' % outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
