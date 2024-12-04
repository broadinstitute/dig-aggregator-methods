import numpy as np
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']

outdir = f'{s3_bioindex}/pigean/{{}}/'
PVALUE_THRESHOLD = 0.05


def bioindex(df, bioindex_name, bioindex_order):
    df.orderBy(bioindex_order) \
        .write \
        .mode('overwrite') \
        .json(outdir.format(bioindex_name))


def top_phewas(df):
    filtered_df = df[df.pValue < PVALUE_THRESHOLD]
    bioindex_order = [col('phenotype'), col('sigma'), col('gene_set_size'), col('pValue').asc()]
    bioindex(filtered_df, 'top_phewas', bioindex_order)


def phewas(df):
    bioindex_order = [col('phenotype'), col('sigma'), col('gene_set_size'), col('factor'), col('pValue').asc()]
    bioindex(df, 'phewas', bioindex_order)


def main():
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = f'{s3_in}/out/pigean/phewas/*/*/*/*/*.json'
    df = spark.read.json(srcdir)

    df = df.withColumn('pValue', when(df.pValue == 0.0, np.nextafter(0, 1)).otherwise(df.pValue))
    df = df.withColumn('pValue_marginal', when(df.pValue_marginal == 0.0, np.nextafter(0, 1)).otherwise(df.pValue_marginal))
    df = df.withColumn('pValue_orig', when(df.pValue_orig == 0.0, np.nextafter(0, 1)).otherwise(df.pValue_orig))
    df = df.withColumn('pValue_robust', when(df.pValue_robust == 0.0, np.nextafter(0, 1)).otherwise(df.pValue_robust))

    top_phewas(df)
    phewas(df)

    spark.stop()


if __name__ == '__main__':
    main()
