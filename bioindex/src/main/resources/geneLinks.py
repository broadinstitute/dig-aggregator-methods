from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = 's3://dig-analysis-data/annotated_regions/target_gene_links/*'
    outdir = 's3://dig-bio-index/regions/gene_links'

    # load all gene prediciton regions
    df = spark.read.json(f'{srcdir}/part-*')

    # sort and write
    df.orderBy(['tissue', 'chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
