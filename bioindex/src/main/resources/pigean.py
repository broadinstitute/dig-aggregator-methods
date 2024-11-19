import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']

outdir = f'{s3_bioindex}/pigean/{{}}/{{}}/'

clean = udf(lambda s: s.replace(',', ';').encode('utf-8').decode('ascii', errors='ignore'))


def attach_max_values(df, fields):
    max_values = df \
        .select('phenotype', 'sigma', 'gene_set_size', *fields) \
        .groupBy(['phenotype', 'sigma', 'gene_set_size']) \
        .agg({field: 'max' for field in fields}) \
        .select('phenotype', 'sigma', 'gene_set_size', *[col(f'max({field})').alias(f'max_trait_{field}') for field in fields])  # rename
    return df.join(max_values, how='left', on=['phenotype', 'sigma', 'gene_set_size'])


def bioindex(df, bioindex_name, bioindices, max_fields):
    for name, order in bioindices.items():
        if len(max_fields) > 0 and name != 'phenotype':
            df_out = attach_max_values(df, max_fields)
        else:
            df_out = df
        df_out.orderBy(order) \
            .write \
            .mode('overwrite') \
            .json(outdir.format(bioindex_name, name))


def gene(spark):
    srcdir = f'{s3_in}/out/pigean/combined_gene_stats/*/*/*/*/*.json'
    bioindices = {
        'gene': [col('trait_group'), col('gene'), col('sigma'), col('gene_set_size'), col('combined').desc()],
        'phenotype': [col('phenotype'), col('sigma'), col('gene_set_size'), col('combined').desc()]
    }
    df = spark.read.json(srcdir)
    df = df.filter(df.gene.isNotNull())
    df = df.withColumn('gene', clean(df.gene))
    bioindex(df, 'gene', bioindices, ['prior', 'combined', 'log_bf'])


def gene_set(spark):
    srcdir = f'{s3_in}/out/pigean/combined_gene_set_stats/*/*/*/*/*.json'
    bioindices = {
        'gene_set': [col('trait_group'), col('gene_set'), col('sigma'), col('gene_set_size'), col('beta').desc()],
        'phenotype': [col('phenotype'), col('sigma'), col('gene_set_size'), col('beta').desc()]
    }
    df = spark.read.json(srcdir)
    df = df.filter(df.gene_set.isNotNull())
    df = df.withColumn('gene_set', clean(df.gene_set))
    bioindex(df, 'gene_set', bioindices, ['beta', 'beta_uncorrected'])


def gene_gene_set(spark):
    srcdir = f'{s3_in}/out/pigean/gene_gene_set_stats/*/*/*/*/*.json'
    bioindices = {
        'gene': [col('phenotype'), col('gene'), col('sigma'), col('gene_set_size'), col('combined').desc()],
        'gene_set': [col('phenotype'), col('gene_set'), col('sigma'), col('gene_set_size'), col('beta').desc()]
    }
    df = spark.read.json(srcdir)
    df = df.filter(df.gene.isNotNull())
    df = df.withColumn('gene', clean(df.gene))
    df = df.filter(df.gene_set.isNotNull())
    df = df.withColumn('gene_set', clean(df.gene_set))
    bioindex(df, 'gene_gene_set', bioindices, [])


def main():
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    gene(spark)
    gene_set(spark)
    gene_gene_set(spark)

    spark.stop()


if __name__ == '__main__':
    main()
