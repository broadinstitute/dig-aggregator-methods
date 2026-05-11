import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']

outdir = f'{s3_bioindex}/single_cell/factor/{{}}/'

clean = udf(lambda s: s.replace(',', ';').encode('utf-8').decode('ascii', errors='ignore'))


def bioindex(df, bioindex_name, bioindex_order):
    df.orderBy(bioindex_order) \
        .write \
        .mode('overwrite') \
        .json(outdir.format(bioindex_name))


def factor(spark):
    srcdir = f'{s3_in}/out/single_cell/factors/*/*/*/factors.json'
    df = spark.read.json(srcdir)
    bioindex_order = [col('dataset'), col('cell_type'), col('model'), col('importance').desc()]
    bioindex(df, 'factor', bioindex_order)


def gene_factor(spark):
    srcdir = f'{s3_in}/out/single_cell/factors/*/*/*/factor_genes.json'
    df = spark.read.json(srcdir)
    df = df.withColumn('gene', clean(df.gene))
    bioindex_order = [col('dataset'), col('cell_type'), col('model'), col('factor'), col('value').desc()]
    bioindex(df, 'gene', bioindex_order)


def cell_factor(spark):
    srcdir = f'{s3_in}/out/single_cell/factors/*/*/*/factor_cells.json'
    df = spark.read.json(srcdir)
    df = df.withColumn('cell', clean(df.cell))
    bioindex_order = [col('dataset'), col('cell_type'), col('model'), col('factor'), col('value').desc()]
    bioindex(df, 'cell', bioindex_order)


def gene_set_factor(spark):
    srcdir = f'{s3_in}/out/single_cell/pigean/*/*/*/gene_set_stats.json'
    df = spark.read.json(srcdir)
    df = df.withColumn('gene_set', clean(df.gene_set))
    bioindex_order = [col('dataset'), col('cell_type'), col('model'), col('factor'), col('beta').desc()]
    bioindex(df, 'gene_set', bioindex_order)


def trait_factor(spark):
    srcdir = f'{s3_in}/out/single_cell/phewas/*/*/*/phewas.json'
    df = spark.read.json(srcdir)
    df = df.withColumn('trait', clean(df.trait))
    df = df.withColumn('trait_name', clean(df.trait_name))
    bioindex_order = [col('dataset'), col('cell_type'), col('model'), col('factor'), col('pValue')]
    bioindex(df, 'trait', bioindex_order)


def main():
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    factor(spark)
    gene_factor(spark)
    cell_factor(spark)
    gene_set_factor(spark)
    trait_factor(spark)

    spark.stop()


if __name__ == '__main__':
    main()
