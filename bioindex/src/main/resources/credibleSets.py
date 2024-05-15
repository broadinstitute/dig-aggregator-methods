import argparse
import os

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import min, max

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    # read all
    srcdir = f'{s3_in}/merged/*/*/part-*'
    outdir = f'{s3_bioindex}/credible_sets/{{}}/{{}}/'

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load all the credible sets for the phenotype
    df = spark.read.json(srcdir)

    df = df\
        .withColumn("chromosome", df["chromosome"].cast("string"))\
        .filter(df.posteriorProbability.isNotNull())
    mixed_df = df[df.ancestry == 'Mixed']
    non_mixed_df = df[df.ancestry != 'Mixed']

    mixed_df \
        .orderBy(['phenotype', 'credibleSetId', 'chromosome', 'position']) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('variants', 'trans-ethnic'))

    non_mixed_df \
        .orderBy(['phenotype', 'ancestry', 'credibleSetId', 'chromosome', 'position']) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('variants', 'ancestry'))

    df = df.select('phenotype', 'ancestry', 'source', 'credibleSetId', 'chromosome', 'clumpStart', 'clumpEnd') \
        .dropDuplicates(['credibleSetId'])
    mixed_df = df[df.ancestry == 'Mixed']
    non_mixed_df = df[df.ancestry != 'Mixed']

    mixed_df \
        .orderBy(['phenotype', 'chromosome', 'clumpStart']) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('locus', 'trans-ethnic'))

    non_mixed_df \
        .orderBy(['phenotype', 'ancestry', 'chromosome', 'clumpStart']) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('locus', 'ancestry'))

    # done
    spark.stop()


if __name__ == '__main__':
    main()
