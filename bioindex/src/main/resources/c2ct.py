import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # filtered (phenotype)
    filtered_srcdir = f'{s3_in}/out/credible_sets/specificity/*/*/*/filtered.*.json'
    unfiltered_srcdir = f'{s3_in}/out/credible_sets/specificity/*/*/*/unfiltered.*.json'
    outdir = f'{s3_bioindex}/credible_sets/c2ct/{{}}'

    filtered_df = spark.read.json(filtered_srcdir)
    filtered_mixed_df = filtered_df[filtered_df['ancestry'] == 'Mixed']
    filtered_non_mixed_df = filtered_df[filtered_df['ancestry'] != 'Mixed']
    unfiltered_df = spark.read.json(unfiltered_srcdir)
    unfiltered_mixed_df = unfiltered_df[unfiltered_df['ancestry'] == 'Mixed']
    unfiltered_non_mixed_df = unfiltered_df[unfiltered_df['ancestry'] != 'Mixed']

    filtered_mixed_df.orderBy([col('phenotype'), col('Q').desc()]) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('trans-ethnic'))

    # sort by phenotype then p-value for global associations
    filtered_non_mixed_df.orderBy([col('phenotype'), col('ancestry'), col('Q').desc()]) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('ancestry'))

    unfiltered_mixed_df.orderBy([col('phenotype'), col('annotation'), col('tissue'), col('biosample'), col('Q').desc()]) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('tissue/trans-ethnic'))

    unfiltered_non_mixed_df.orderBy([col('phenotype'), col('ancestry'), col('annotation'), col('tissue'), col('biosample'), col('Q').desc()]) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('tissue/ancestry'))

    unfiltered_mixed_df.orderBy([col('phenotype'), col('credibleSetId'), col('Q').desc()]) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('credible-set/trans-ethnic'))

    unfiltered_non_mixed_df.orderBy([col('phenotype'), col('ancestry'), col('credibleSetId'), col('Q').desc()]) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('credible-set/ancestry'))

    # done
    spark.stop()


if __name__ == '__main__':
    main()
