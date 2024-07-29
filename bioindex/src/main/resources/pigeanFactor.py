import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']

outdir = f'{s3_bioindex}/pigean/{{}}/'


def bioindex(df, bioindex_name, bioindex_order):
    df.orderBy(bioindex_order) \
        .write \
        .mode('overwrite') \
        .json(outdir.format(bioindex_name))


def factor(spark):
    srcdir = f'{s3_in}/out/pigean/factor/*/*/*/*.json'
    df = spark.read.json(srcdir)
    df = df.withColumn('cluster', df.factor)
    bioindex_order = [col('phenotype'), col('sigma'), col('gene_set_size'), col('gene_set_score').desc()]
    bioindex(df, 'factor', bioindex_order)


def gene_factor(spark):
    srcdir = f'{s3_in}/out/pigean/gene_factor/*/*/*/*.json'
    df = spark.read.json(srcdir)
    bioindex_order = [col('phenotype'), col('sigma'), col('gene_set_size'), col('factor'), col('factor_value').desc()]
    bioindex(df, 'gene_factor', bioindex_order)


def gene_set_factor(spark):
    srcdir = f'{s3_in}/out/pigean/gene_set_factor/*/*/*/*.json'
    df = spark.read.json(srcdir)
    bioindex_order = [col('phenotype'), col('sigma'), col('gene_set_size'), col('factor'), col('factor_value').desc()]
    bioindex(df, 'gene_set_factor', bioindex_order)


def main():
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    factor(spark)
    gene_factor(spark)
    gene_set_factor(spark)

    spark.stop()


if __name__ == '__main__':
    main()
