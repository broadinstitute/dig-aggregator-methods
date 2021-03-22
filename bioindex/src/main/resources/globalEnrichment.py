from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output directories
    srcdir = f's3://dig-analysis-data/out/gregor/enrichment/part-*'
    outdir = f's3://dig-bio-index/global_enrichment'

    # load the global enrichment summaries
    df = spark.read.json(srcdir)

    # sort by phenotype and then p-value
    df.orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
