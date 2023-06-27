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
        .json(f'{outdir}/gene')

    # Want to calculate a mean Tpm value for tissue/gene from dataset values
    aggregate_df = df \
        .select(['tissue', 'gene', 'meanTpm', 'nSamples'])

    aggregate_df = df \
        .withColumn('totalTpm', aggregate_df.meanTpm * aggregate_df.nSamples) \
        .drop('meanTpm') \
        .groupBy(['tissue', 'gene']) \
        .agg({'totalTpm': 'sum', 'nSamples': 'sum'}) \
        .withColumnRenamed('sum(totalTpm)', 'totalTpm') \
        .withColumnRenamed('sum(nSamples)', 'nSamples')

    aggregate_df = aggregate_df \
        .withColumn('meanTpm', aggregate_df.totalTpm / aggregate_df.nSamples) \
        .drop('totalTpm')

    # sort and write
    aggregate_df.orderBy(['tissue']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/tissue')

    # done
    spark.stop()


if __name__ == '__main__':
    main()
