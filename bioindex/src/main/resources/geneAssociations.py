from pyspark.sql import SparkSession

# NOTE: This fails to run on the 52k because spark < 3.0 has a bug and
#       erroneously complains that there are multiple 'pvalue' columns
#       in the ouput.
#
#       The data is very small and this can be run locally with Spark 3.0
#       installed and then just copied back up to s3.


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
        .json('%s/52k' % outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
