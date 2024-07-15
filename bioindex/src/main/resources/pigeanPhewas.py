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


def phewas(spark):
    srcdir = f'{s3_in}/out/pigean/phewas/*/*/*/*.json'
    bioindex_order = [col('phenotype'), col('sigma'), col('gene_set_size'), col('factor'), col('pValue').asc()]
    bioindex(spark, srcdir, 'phewas', bioindex_order)


def main():
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    phewas(spark)

    spark.stop()


if __name__ == '__main__':
    main()
