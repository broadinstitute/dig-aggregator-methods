from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load and output directory
    srcdir = f's3://dig-analysis-data/gene_associations/*/*/'
    outdir = f's3://dig-bio-index/gene_associations'

    # load all the gene associations across all datasets + phenotypes
    df = spark.read.json(srcdir)

    # sort by gene, then by p-value
    df.orderBy(['gene', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json('%s/gene' % outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
