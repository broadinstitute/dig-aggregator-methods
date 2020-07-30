from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output directories
    srcdir = f's3://dig-analysis-data/out/gregor/regions/joined/part-*'
    outdir = f's3://dig-bio-index/regions'

    # load all regions, tissues, and join
    df = spark.read.json(srcdir)

    # join with tissues, sort, and write
    df.orderBy(['annotation', 'chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/annotation')

    # done
    spark.stop()


if __name__ == '__main__':
    main()
