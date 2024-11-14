import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']

outdir = f'{s3_bioindex}/pigean/{{}}/'


def main():
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = f'{s3_in}/out/pigean/graph/*/*/*/*.json'
    df = spark.read.json(srcdir)
    df.orderBy([col('phenotype'), col('sigma'), col('gene_set_size')]) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('graph'))

    spark.stop()


if __name__ == '__main__':
    main()
