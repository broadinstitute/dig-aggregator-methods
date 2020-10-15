from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import size, explode


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load and output directory
    srcdir = f's3://dig-analysis-data/out/metaanalysis/trans-ethnic'
    outdir = f's3://dig-bio-index/associations/bloom'

    # load all trans-ethnic, meta-analysis results for all variants
    associations = spark.read.json(f'{srcdir}/*/part-*')
    associations = associations.filter(associations.pValue < 0.01) \
        .select(
            associations.varId,
            associations.pValue,
            associations.phenotype,
        )

    # create a single frame with all the phenotypes in an array per variant
    bloom = associations.rdd \
        .keyBy(lambda r: r.varId) \
        .combineByKey(
            lambda a: [a.phenotype],
            lambda a, b: a + [b.phenotype],
            lambda a, b: a + b,
        ) \
        .map(lambda r: Row(varId=r[0], bloom=r[1])) \
        .toDF()

    # get only the globally significant associations
    df = associations.filter(associations.pValue <= 5e-8)

    # join with the bloom filter
    df = df.join(bloom, on='varId', how='inner')

    # remove any variants where the primary phenotype is the only one
    df = df.filter(size(df.bloom) > 1)

    # sort by phenotype, then pValue, and write the filter
    df.orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
