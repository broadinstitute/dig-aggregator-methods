from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load and output directory
    srcdir = f's3://dig-analysis-data/out/burdenbinning'
    outdir = f's3://dig-bio-index/burden'

    # load all trans-ethnic, meta-analysis results for all variants
    df = spark.read.json(f'{srcdir}/part-*')

    # sort by gene and then bin
    df.orderBy(['geneSymbol', 'burdenBinId']) \
        .write \
        .mode('overwrite') \
        .json('%s/gene' % outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
