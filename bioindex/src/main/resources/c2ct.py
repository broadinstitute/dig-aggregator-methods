import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output locations
    srcdir = f'{s3_in}/out/credible_sets/specificity/*/*/*/*.json'
    outdir = f'{s3_bioindex}/credible_sets/c2ct/{{}}'

    df = spark.read.json(srcdir)

    # partition dataframe
    mixed_df = df[df['ancestry'] == 'Mixed']
    non_mixed_df = df[df['ancestry'] != 'Mixed']

    mixed_df.orderBy([col('phenotype'), col('Q').desc()]) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('trans-ethnic'))

    # sort by phenotype then p-value for global associations
    non_mixed_df.orderBy([col('phenotype'), col('ancestry'), col('Q').desc()]) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('ancestry'))

    mixed_df.orderBy([col('phenotype'), col('annotation'), col('tissue'), col('Q').desc()]) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('tissue/trans-ethnic'))

    non_mixed_df.orderBy([col('phenotype'), col('ancestry'), col('annotation'), col('tissue'), col('Q').desc()]) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('tissue/ancestry'))

    mixed_df.orderBy([col('phenotype'), col('credibleSetId'), col('Q').desc()]) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('credible-set/trans-ethnic'))

    non_mixed_df.orderBy([col('phenotype'), col('credibleSetId'), col('Q').desc()]) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('credible-set/ancestry'))

    # done
    spark.stop()


if __name__ == '__main__':
    main()
