import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, lit, rank, udf, when
from pyspark.sql.window import Window

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']

outdir = f'{s3_bioindex}/pigean/{{}}/{{}}'


def attach_max_values(df, fields):
    max_values = df \
        .select('phenotype', *fields) \
        .groupBy(['phenotype']) \
        .agg({field: 'max' for field in fields}) \
        .select('phenotype', *[col(f'max({field})').alias(f'max_trait_{field}') for field in fields])  # rename
    return df.join(max_values, how='left', on='phenotype')


def bioindex(spark, srcdir, bioindex_name, bioindices, max_fields):
    df = spark.read.json(srcdir)

    for name, order in bioindices.items():
        if len(max_fields) > 0 and name not in ['phenotype', 'source']:
            df_out = attach_max_values(df, max_fields)
        else:
            df_out = df

        if 'overall' in name:
            df_out = df_out.filter(df_out.beta_uncorrected > 0)
        df_out.orderBy(order) \
            .write \
            .mode('overwrite') \
            .json(outdir.format(bioindex_name, name))


def gene(spark):
    srcdir = f'{s3_in}/out/pigean/gene_stats/*/*/*/*.json'
    bioindices = {
        'gene': [col('gene'), col('combined').desc()],
        'phenotype': [col('phenotype'), col('combined').desc()]
    }
    bioindex(spark, srcdir, 'gene', bioindices, ['prior', 'combined', 'log_bf'])


def gene_set(spark):
    srcdir = f'{s3_in}/out/pigean/gene_set_stats/*/*/*/*.json'
    bioindices = {
        'gene_set': [col('gene_set'), col('beta_uncorrected').desc()],
        'phenotype': [col('phenotype'), col('beta_uncorrected').desc()]
    }
    bioindex(spark, srcdir, 'gene_set', bioindices, ['beta', 'beta_uncorrected'])


def gene_set_source(spark):
    srcdir = f'{s3_in}/out/pigean/gene_set_stats/*/*/*/*.json'

    df = spark.read.json(srcdir)
    df = df.withColumn('source_index', df.source)

    source_partition = Window.partitionBy('source').orderBy('beta_uncorrected')
    source_df = df.withColumn('rank', rank().over(source_partition))
    source_df = source_df.filter(source_df.rank <= 1000).drop('rank')
    source_df = source_df.withColumn('source_index', lit('all'))
    df = df.union(source_df)

    df.orderBy(col('source_index'), col('beta_uncorrected').desc()) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('gene_set', 'source'))


def gene_gene_set(spark):
    srcdir = f'{s3_in}/out/pigean/gene_gene_set_stats/*/*/*/*.json'
    bioindices = {
        'gene': [col('phenotype'), col('gene'), col('gene_set_size'), col('combined').desc()],
        'gene_set': [col('phenotype'), col('gene_set'), col('gene_set_size'), col('beta').desc()],
        'overall_gene': [col('gene'), col('gene_set_size'), col('beta_uncorrected').desc()]
    }
    bioindex(spark, srcdir, 'gene_gene_set', bioindices, [])


def main():
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    gene(spark)
    gene_set(spark)
    gene_gene_set(spark)
    gene_set_source(spark)

    spark.stop()


if __name__ == '__main__':
    main()
