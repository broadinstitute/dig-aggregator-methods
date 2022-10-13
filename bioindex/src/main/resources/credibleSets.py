import argparse

from pyspark.sql import SparkSession, Row


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

    # TODO: Some old datasets have chromosome as integers. Better solution is to reupload with str chr, but cast for now
    # TODO: Eventually, also filter otu null posterior probabilities as well
    df = df\
        .withColumn("chromosome", df["chromosome"].cast("string"))\
        .filter(df.posteriorProbability.isNotNull())

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

    # done
    spark.stop()


if __name__ == '__main__':
    main()
