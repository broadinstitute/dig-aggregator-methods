import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']

outdir = f'{s3_bioindex}/pigean/{{}}/'

clean = udf(lambda s: s.replace(',', ';').encode('utf-8').decode('ascii', errors='ignore'))


def bioindex(df, bioindex_name, bioindex_order):
    df.orderBy(bioindex_order) \
        .write \
        .mode('overwrite') \
        .json(outdir.format(bioindex_name))


def factor(spark):
    srcdir = f'{s3_in}/out/pigean/factor/*/*/*/*.json'
    df = spark.read.json(srcdir)
    df = df.withColumn('cluster', df.factor)
    df = df.withColumn('top_genes', clean(df.top_genes))
    df = df.withColumn('top_gene_sets', clean(df.top_gene_sets))
    bioindex_order = [col('phenotype'), col('gene_set_size'), col('gene_set_score').desc()]
    bioindex(df, 'factor', bioindex_order)


def gene_factor(spark):
    srcdir = f'{s3_in}/out/pigean/gene_factor/*/*/*/*.json'
    df = spark.read.json(srcdir)
    df = df.withColumn('gene', clean(df.gene))
    bioindex_order = [col('phenotype'), col('gene_set_size'), col('factor'), col('factor_value').desc()]
    bioindex(df, 'gene_factor', bioindex_order)


def gene_set_factor(spark):
    srcdir = f'{s3_in}/out/pigean/gene_set_factor/*/*/*/*.json'
    df = spark.read.json(srcdir)
    df = df.withColumn('gene_set', clean(df.gene_set))
    bioindex_order = [col('phenotype'), col('gene_set_size'), col('factor'), col('factor_value').desc()]
    bioindex(df, 'gene_set_factor', bioindex_order)


def factor_gene(spark):
    srcdir = f'{s3_in}/out/old-pigean/factor/*/*/*/*/*.json'
    factor_df = spark.read.json(srcdir)
    factor_df = factor_df.withColumn('top_genes', clean(factor_df.top_genes))
    factor_df = factor_df.withColumn('top_gene_sets', clean(factor_df.top_gene_sets))

    srcdir = f'{s3_in}/out/old-pigean/gene_factor/*/*/*/*/*.json'
    gene_df = spark.read.json(srcdir)
    gene_df = gene_df.withColumn('gene', clean(gene_df.gene)) \
        .select('phenotype', 'gene_set_size', 'factor', 'gene', 'factor_value')

    df = gene_df.join(factor_df, ['phenotype', 'gene_set_size', 'factor'])
    bioindex_order = [col('phenotype'), col('gene_set_size'), col('gene'), col('factor_value').desc()]
    bioindex(df, 'factor_gene', bioindex_order)


def main():
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    factor(spark)
    gene_factor(spark)
    gene_set_factor(spark)
    factor_gene(spark)

    spark.stop()


if __name__ == '__main__':
    main()
