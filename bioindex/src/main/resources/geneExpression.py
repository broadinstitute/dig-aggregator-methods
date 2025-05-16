import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, log2

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def get_q(df):
    df_gene_annot = df \
        .withColumn('weightedTpm', df.meanTpm * df.nSamples) \
        .groupBy(['gene', 'tissue', 'biosample']) \
        .agg({'weightedTpm': 'sum', 'nSamples': 'sum'})
    df_gene_annot = df_gene_annot \
        .withColumn('meanTpm', df_gene_annot['sum(weightedTpm)'] / df_gene_annot['sum(nSamples)']) \
        .drop('weightedTpm', 'sum(weightedSum)', 'sum(nSamples)')
    df_gene = df_gene_annot \
        .groupBy(['gene']) \
        .agg({'meanTpm': 'sum'})
    df_joined = df_gene_annot.join(df_gene, 'gene')
    df_joined = df_joined \
        .withColumn('p', df_joined['meanTpm'] / df_joined['sum(meanTpm)']) \
        .drop('sum(meanTpm)')
    df_joined = df_joined \
        .withColumn('H', -df_joined.p * log2(df_joined.p))
    df_joined = df_joined \
        .withColumn('Q', df_joined.H - log2(df_joined.p))
    df_joined = df_joined \
        .filter((df_joined.Q.isNotNull()))
    max_Q = df_joined.agg({"Q": "max"}).head()["max(Q)"]
    df_joined = df_joined \
        .withColumn('mESI', 1 - df_joined.Q / max_Q)
    df_joined = df_joined \
        .groupBy(['gene', 'tissue']) \
        .agg({'mESI': 'max', 'H': 'min'}) \
        .withColumnRenamed('max(mESI)', 'Q') \
        .withColumnRenamed('min(H)', 'H')
    return df_joined


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = f'{s3_in}/annotated_regions/gene_expression_levels/*'
    outdir = f'{s3_bioindex}/regions/gene_expression'

    # load all variant prediciton regions
    df = spark.read.json(f'{srcdir}/part-*')

    # Need to make sure these values are floats/ints and filter out if not
    df = df.withColumn('minTpm', col('minTpm').cast('float')) \
        .withColumn('firstQuTpm', col('firstQuTpm').cast('float')) \
        .withColumn('medianTpm', col('medianTpm').cast('float')) \
        .withColumn('meanTpm', col('meanTpm').cast('float')) \
        .withColumn('thirdQuTpm', col('thirdQuTpm').cast('float')) \
        .withColumn('maxTpm', col('maxTpm').cast('float')) \
        .withColumn('nSamples', col('nSamples').cast('int'))

    df = df.filter(df.minTpm.isNotNull()) \
        .filter(df.firstQuTpm.isNotNull()) \
        .filter(df.medianTpm.isNotNull()) \
        .filter(df.meanTpm.isNotNull()) \
        .filter(df.thirdQuTpm.isNotNull()) \
        .filter(df.maxTpm.isNotNull()) \
        .filter(df.nSamples.isNotNull())

    df_q = get_q(df)

    # sort and write
    df.join(df_q, on=['gene', 'tissue']) \
        .orderBy(['gene']) \
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
    aggregate_df.join(df_q, on=['gene', 'tissue']) \
        .orderBy(['tissue']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/tissue')

    # done
    spark.stop()


if __name__ == '__main__':
    main()
