import argparse
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import when

OUTDIR = 's3://dig-bio-index'


def process_gene_datasets(spark):
    """
    Load all 52k results and write them out both sorted by gene and by
    phenotype, so they may be queried either way.
    """
    df = spark.read.json('s3://dig-analysis-data/gene_associations/52k_*/*/part-*')

    df = df.withColumn('pValue', when(df.pValue == 0.0, np.nextafter(0, 1)).otherwise(df.pValue))
    genes = spark.read.json('s3://dig-analysis-data/genes/GRCh37/part-*')

    # fix for join
    genes = genes.select(
        genes.name.alias('gene'),
        genes.chromosome,
        genes.start,
        genes.end,
        genes.type,
    )

    df = df.join(genes, on='gene', how='inner')

    # sort by gene, then by p-value
    df.orderBy(['gene', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f'{OUTDIR}/gene_associations/52k')

    # sort by phenotype, then by p-value for the gene finder
    df.orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json('s3://dig-bio-index/finder/52k')


def process_600trait_datasets(spark):
    """
    Load all 600trait results and write them out both sorted by gene and by
    phenotype, so they may be queried either way.
    """
    df = spark.read.json('s3://dig-analysis-data/gene_associations/600k_600traits/*/*/part-*')

    df = df.withColumn('pValue', when(df.pValue == 0.0, np.nextafter(0, 1)).otherwise(df.pValue))
    genes = spark.read.json('s3://dig-analysis-data/genes/GRCh37/part-*')

    # fix for join
    genes = genes.select(
        genes.name.alias('ensemblId'),
        genes.chromosome,
        genes.start,
        genes.end,
        genes.type,
    )

    df = df.join(genes, on='ensemblId', how='inner')

    # sort by gene, then by p-value
    df.orderBy(['ancestry', 'gene', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f'{OUTDIR}/gene_associations/600trait')

    # sort by phenotype, then by p-value for the gene finder
    df.drop('masks') \
        .orderBy(['ancestry', 'phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json('s3://dig-bio-index/finder/600trait')


def process_transcript_datasets(spark):
    """
    Load all 52k results and write them out both sorted by gene and by
    phenotype, so they may be queried either way.
    """
    df = spark.read.json('s3://dig-analysis-data/transcript_associations/*/*/part-*')

    # sort by gene, then by p-value
    df.orderBy(['transcript', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f'{OUTDIR}/transcript_associations/55k')


def process_magma(spark):
    """
    Load the MAGMA results and write them out both sorted by gene and by
    phenotype, so they may be queried either way.
    """
    df = spark.read.json('s3://dig-analysis-data/out/magma/gene-associations/*/*/')
    genes = spark.read.json('s3://dig-analysis-data/genes/GRCh37/part-*')

    # fix for join
    genes = genes.select(
        genes.name.alias('gene'),
        genes.chromosome,
        genes.start,
        genes.end,
        genes.type,
    )

    # partition dataframe
    mixed_df = df[df['ancestry'] == 'Mixed']
    non_mixed_df = df[df['ancestry'] != 'Mixed']

    # join with genes for region data
    mixed_df = mixed_df.join(genes, on='gene', how='inner')
    non_mixed_df = non_mixed_df.join(genes, on='gene', how='inner')

    # sort by gene, then by p-value
    mixed_df.orderBy(['gene', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f'{OUTDIR}/gene_associations/gene/trans-ethnic')

    # sort by phenotype, then by p-value for the gene finder
    mixed_df.orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f's3://dig-bio-index/finder/gene/trans-ethnic')

    # sort by gene, ancestry, then by p-value
    non_mixed_df.orderBy(['gene', 'ancestry', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f'{OUTDIR}/gene_associations/gene/ancestry-specific')

    # sort by phenotype, ancestry, then by p-value for the gene finder
    non_mixed_df.orderBy(['phenotype', 'ancestry', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f's3://dig-bio-index/finder/gene/ancestry-specific')


def main():
    """
    Arguments: --52k | --magma / --transcript
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('--52k', action='store_true', dest='flag_52k')
    opts.add_argument('--600trait', action='store_true', dest='flag_600trait')
    opts.add_argument('--magma', action='store_true')
    opts.add_argument('--transcript', action='store_true')

    # parse CLI flags
    args = opts.parse_args()

    # initialize spark
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    if args.flag_52k:
        process_gene_datasets(spark)
    if args.flag_600trait:
        process_600trait_datasets(spark)
    if args.magma:
        process_magma(spark)
    if args.transcript:
        process_transcript_datasets(spark)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
