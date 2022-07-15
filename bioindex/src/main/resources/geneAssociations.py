import argparse

from pyspark.sql import SparkSession

OUTDIR = 's3://dig-bio-index/gene_associations'


def process_datasets(spark):
    """
    Load all 52k results and write them out both sorted by gene and by
    phenotype, so they may be queried either way.
    """
    df = spark.read.json('s3://dig-analysis-data/gene_associations/*/*/part-*')

    # sort by gene, then by p-value
    df.orderBy(['gene', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json('%s/52k' % OUTDIR)

    # sort by phenotype, then by p-value for the gene finder
    df.orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json('s3://dig-bio-index/finder/52k')


def process_magma(spark):
    """
    Load the MAGMA results and write them out both sorted by gene and by
    phenotype, so they may be queried either way.
    """
    df = spark.read.json('s3://dig-analysis-data/out/magma/gene-associations/*/')
    genes = spark.read.json('s3://dig-analysis-data/genes/GRCh37/part-*')

    # fix for join
    genes = genes.select(
        genes.name.alias('gene'),
        genes.chromosome,
        genes.start,
        genes.end,
        genes.type,
    )

    # join with genes for region data
    df = df.join(genes, on='gene', how='inner')

    # sort by gene, then by p-value
    df.orderBy(['gene', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json('%s/gene' % OUTDIR)

    # sort by phenotype, then by p-value for the gene finder
    df.orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json('s3://dig-bio-index/finder/gene')


def main():
    """
    Arguments: --52k | --magma
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('--52k', action='store_true', dest='flag_52k')
    opts.add_argument('--magma', action='store_true')

    # parse CLI flags
    args = opts.parse_args()

    # initialize spark
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    if args.flag_52k:
        process_datasets(spark)
    if args.magma:
        process_magma(spark)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
