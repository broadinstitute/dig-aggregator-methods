from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output locations
    srcdir = f's3://dig-analysis-data/out/metaanalysis/top/*/part-*'
    outdir = f's3://dig-bio-index/associations'

    # load the top associations for each phenotype
    df = spark.read.json(srcdir)

    # sort all top associations together by position
    df.orderBy(['chromosome', 'clumpStart']) \
        .write \
        .mode('overwrite') \
        .json('%s/top' % outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
