import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']

outdir = f'{s3_bioindex}/pigean/{{}}/'


def bioindex(spark, srcdir, bioindex_name, bioindex_order):
    df = spark.read.json(srcdir)
    df.orderBy(bioindex_order) \
        .write \
        .mode('overwrite') \
        .json(outdir.format(bioindex_name))


def factor(spark):
    srcdir = f'{s3_in}/out/pigean/factor/*/*/*/*.json'
    bioindex_order = [col('phenotype'), col('sigma'), col('gene_set_size'), col('gene_score').desc()]
    bioindex(spark, srcdir, 'factor', bioindex_order)


def gene_factor(spark):
    srcdir = f'{s3_in}/out/pigean/gene_factor/*/*/*/*.json'
    bioindex_order = [col('phenotype'), col('sigma'), col('gene_set_size'), col('factor'), col('factor_value').desc()]
    bioindex(spark, srcdir, 'gene_factor', bioindex_order)


def gene_set_factor(spark):
    srcdir = f'{s3_in}/out/pigean/gene_set_factor/*/*/*/*.json'
    bioindex_order = [col('phenotype'), col('sigma'), col('gene_set_size'), col('factor'), col('factor_value').desc()]
    bioindex(spark, srcdir, 'gene_set_factor', bioindex_order)


def main():
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    factor(spark)
    gene_factor(spark)
    gene_set_factor(spark)

    spark.stop()


if __name__ == '__main__':
    main()
