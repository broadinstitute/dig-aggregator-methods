import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']
outdir = f'{s3_bioindex}/credible_sets/c2ct/{{}}/{{}}'


def build_bioindex(spark, key, bioindex_order):
    srcdir = f'{s3_in}/out/credible_sets/specificity/*/*/{key}.*.json'
    df = spark.read.json(srcdir)
    mixed_df = df[df['ancestry'] == 'Mixed']
    non_mixed_df = df[df['ancestry'] != 'Mixed']
    mixed_df.orderBy([col('phenotype')] + bioindex_order) \
        .write \
        .mode('overwrite') \
        .json(outdir.format(key, 'trans-ethnic'))
    non_mixed_df.orderBy([col('phenotype'), col('ancestry')] + bioindex_order) \
        .write \
        .mode('overwrite') \
        .json(outdir.format(key, 'ancestry'))


def main():
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    build_bioindex(spark, 'all', [col('Q').desc()])
    build_bioindex(spark, 'annotation', [col('annotation'), col('Q').desc()])
    build_bioindex(spark, 'tissue', [col('annotation'), col('tissue'), col('Q').desc()])
    build_bioindex(spark, 'biosample', [col('annotation'), col('tissue'), col('biosample'), col('Q').desc()])
    build_bioindex(spark, 'credible_set_id', [col('credibleSetId'), col('Q').desc()])

    # done
    spark.stop()


if __name__ == '__main__':
    main()
