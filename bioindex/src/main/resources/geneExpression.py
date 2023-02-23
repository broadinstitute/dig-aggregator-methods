from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = 's3://dig-analysis-data/annotated_regions/gene_expression_levels/*'
    outdir = 's3://dig-bio-index/regions/gene_expression'

    # load all variant prediciton regions
    df = spark.read.json(f'{srcdir}/part-*')

    # sort and write
    df.orderBy(['gene']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
