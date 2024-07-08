import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']

outdir = f'{s3_bioindex}/pigean/{{}}/{{}}/{{}}/'


def attach_max_values(df, fields):
    max_values = df \
        .select('phenotype', 'sigma', 'gene_set_size', *fields) \
        .groupBy(['phenotype', 'sigma', 'gene_set_size']) \
        .agg({field: 'max' for field in fields}) \
        .select('phenotype', 'sigma', 'gene_set_size', *[col(f'max({field})').alias(f'max_trait_{field}') for field in fields])  # rename
    return df.join(max_values, how='left', on=['phenotype', 'sigma', 'gene_set_size'])


def bioindex(spark, srcdir, bioindex_name, bioindices, max_fields):
    df = spark.read.json(srcdir)
    for name, order in bioindices.items():
        if len(max_fields) > 0 and name != 'phenotype':
            df_out = attach_max_values(df, max_fields)
        else:
            df_out = df
        df_out.orderBy(order) \
            .write \
            .mode('overwrite') \
            .json(outdir.format(bioindex_name, name, 'all'))


def gene(spark):
    srcdir = f'{s3_in}/out/pigean/gene_stats/*/*/*/*.json'
    bioindices = {
        'gene': [col('gene'), col('sigma'), col('gene_set_size'), col('combined').desc()],
        'phenotype': [col('phenotype'), col('sigma'), col('gene_set_size'), col('combined').desc()]
    }
    bioindex(spark, srcdir, 'gene', bioindices, ['prior', 'combined', 'log_bf'])


def gene_set(spark):
    srcdir = f'{s3_in}/out/pigean/gene_set_stats/*/*/*/*.json'
    bioindices = {
        'gene_set': [col('gene_set'), col('sigma'), col('gene_set_size'), col('beta').desc()],
        'phenotype': [col('phenotype'), col('sigma'), col('gene_set_size'), col('beta').desc()]
    }
    bioindex(spark, srcdir, 'gene_set', bioindices, ['beta', 'beta_uncorrected'])


def gene_gene_set(spark):
    srcdir = f'{s3_in}/out/pigean/gene_gene_set_stats/*/*/*/*.json'
    bioindices = {
        'gene': [col('phenotype'), col('gene'), col('sigma'), col('gene_set_size'), col('combined').desc()],
        'gene_set': [col('phenotype'), col('gene_set'), col('sigma'), col('gene_set_size'), col('beta').desc()]
    }
    bioindex(spark, srcdir, 'gene_gene_set', bioindices, [])


def main():
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    gene(spark)
    gene_set(spark)
    gene_gene_set(spark)

    spark.stop()


if __name__ == '__main__':
    main()
