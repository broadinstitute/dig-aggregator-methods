import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--build', type=str, required=True)
    args = opts.parse_args()

    srcdir = f's3://dig-analysis-bin/genes/{args.build}/part-*'
    outdir = f'{s3_bioindex}/genes/{{}}/{args.build}'

    # all valid chromosomes
    chromosomes = list(map(str, range(1, 23))) + ['X', 'Y', 'XY', 'MT']

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load the genes and write them sorted
    df = spark.read.json(srcdir) \
        .filter(col('chromosome').isin(*chromosomes)) \
        .withColumn('build', lit(args.build))

    # save into two places for different bioindices
    for bioindex_type in ['default', 'build']:
        df.coalesce(1) \
            .orderBy(['chromosome', 'start']) \
            .write \
            .mode('overwrite') \
            .json(outdir.format(bioindex_type))

    # done
    spark.stop()


if __name__ == '__main__':
    main()
