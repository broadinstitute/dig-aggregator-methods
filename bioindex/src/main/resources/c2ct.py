import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # filtered (phenotype)
    filtered_srcdir = f'{s3_in}/out/credible_sets/specificity/*/*/*/filtered.*.json'
    filtered_outdir = f'{s3_bioindex}/credible_sets/c2ct/{{}}'

    filtered_df = spark.read.json(filtered_srcdir)

    filtered_df[filtered_df['ancestry'] == 'Mixed'] \
        .orderBy([col('phenotype'), col('Q').desc()]) \
        .write \
        .mode('overwrite') \
        .json(filtered_outdir.format('trans-ethnic'))

    # sort by phenotype then p-value for global associations
    filtered_df[filtered_df['ancestry'] != 'Mixed'] \
        .orderBy([col('phenotype'), col('ancestry'), col('Q').desc()]) \
        .write \
        .mode('overwrite') \
        .json(filtered_outdir.format('ancestry'))

    # tissue
    unfiltered_srcdir = f'{s3_in}/out/credible_sets/specificity/*/*/*/unfiltered.*.json'
    unfiltered_outdir = f'{s3_bioindex}/credible_sets/c2ct-unfiltered/{{}}'

    unfiltered_df = spark.read.json(unfiltered_srcdir)

    unfiltered_df[unfiltered_df['ancestry'] == 'Mixed'] \
        .orderBy([col('phenotype'), col('annotation'), col('tissue'), col('Q').desc()]) \
        .write \
        .mode('overwrite') \
        .json(unfiltered_outdir.format('trans-ethnic'))

    # sort by phenotype then p-value for global associations
    unfiltered_df[unfiltered_df['ancestry'] != 'Mixed'] \
        .orderBy([col('phenotype'), col('ancestry'), col('annotation'), col('tissue'), col('Q').desc()]) \
        .write \
        .mode('overwrite') \
        .json(unfiltered_outdir.format('ancestry'))

    # done
    spark.stop()


if __name__ == '__main__':
    main()
