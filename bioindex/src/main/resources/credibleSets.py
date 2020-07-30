import argparse

from pyspark.sql import SparkSession, Row
from pyspark.sql.window import Window
from pyspark.sql.functions import col, desc, rank


def main():
    """
    Arguments: phenotype
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')

    # parse cmdline
    args = opts.parse_args()

    # read all
    srcdir = f's3://dig-analysis-data/credible_sets/*/{args.phenotype}/part-*'
    outdir = f's3://dig-bio-index/credible_sets'

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load all the credible sets for the phenotype
    df = spark.read.json(srcdir)

    # sort by credible set and then locus
    df.coalesce(1) \
        .orderBy(['credibleSetId', 'chromosome', 'position']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/variants/{args.phenotype}')

    # sort credible set IDs by region
    df.rdd \
        .keyBy(lambda r: (r.phenotype, r.dataset, r.credibleSetId, r.chromosome)) \
        .combineByKey(
            lambda r: (r.position, r.position),
            lambda a, b: (min(a[0], b.position), max(a[1], b.position)),
            lambda a, b: (min(a[0], b[0]), max(a[1], b[1])),
        ) \
        .map(lambda r: Row(
            phenotype=r[0][0],
            dataset=r[0][1],
            credibleSetId=r[0][2],
            chromosome=r[0][3],
            start=r[1][0],
            end=r[1][1]
        )) \
        .toDF() \
        .coalesce(1) \
        .orderBy(['chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/locus/{args.phenotype}')

    # load joined regions
    regions = spark.read.json('s3://dig-analysis-data/out/gregor/regions/joined/part-*')

    # for each credible find regions that overlap the top variants
    w = Window \
        .partitionBy(['dataset', 'credibleSetId']) \
        .orderBy(desc('posteriorProbability'))

    # get the ranking of each variant in the credible set
    df = df \
        .withColumn('rank', rank().over(w)) \
        .filter((col('rank') <= 20) | (col('posteriorProbability') > 0.002)) \
        .select(
            col('phenotype'),
            col('dataset'),
            col('credibleSetId'),
            col('chromosome'),
            col('position'),
        )

    # overlap condition
    on = (df.chromosome == regions.chromosome) & \
         (df.position >= regions.start) & \
         (df.position < regions.end)

    # for each credible set, find regions that overlap the top variants
    df = df.join(regions.alias('regions'), on=on) \
        .select(
            col('phenotype'),
            col('dataset'),
            col('credibleSetId'),
            col('regions.chromosome').alias('chromosome'),
            col('regions.start').alias('start'),
            col('regions.end').alias('end'),
            col('regions.tissueId').alias('tissueId'),
            col('regions.tissue').alias('tissue'),
            col('regions.annotation').alias('annotation'),
            col('regions.method').alias('method'),
            col('regions.predictedTargetGene').alias('predictedTargetGene'),
            col('regions.targetStart').alias('targetStart'),
            col('regions.targetEnd').alias('targetEnd'),
            col('regions.transcriptionStartSite').alias('transcriptionStartSite'),
        )

    # drop the variant position, sort by credible set id,
    df.orderBy(['credibleSetId']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/regions/{args.phenotype}')

    # done
    spark.stop()


if __name__ == '__main__':
    main()
