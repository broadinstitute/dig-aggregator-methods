#!/usr/bin/python3
import argparse
import platform

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

S3DIR = 's3://dig-analysis-data'

SCHEMA = StructType(
    [
        StructField('chromosome', StringType()),
        StructField('start', IntegerType()),
        StructField('end', IntegerType()),
    ]
)


def main():
    """
    Arguments: partition
    """
    print('Python version: %s' % platform.python_version())

    opts = argparse.ArgumentParser()
    opts.add_argument('partition')

    # extract the dataset from the command line
    args = opts.parse_args()

    # get the source and output directories
    srcdir = f'{S3DIR}/annotated_regions/*/part-*'
    outdir = f'{S3DIR}/out/gregor/regions/merged/'

    srcdir = f'{S3DIR}/out/gregor/regions/partitioned/*/*/partition={args.partition}/part-*'
    outdir = f'{S3DIR}/out/gregor/regions/merged/partition={args.partition}/'

    # create a spark session
    spark = SparkSession.builder.appName('gregor').getOrCreate()

    # read all the fields needed across the regions for the dataset
    df = spark.read.csv(srcdir, sep='\t', header=False, schema=SCHEMA)

    # sort by position, then write as a single file
    df = df.coalesce(1) \
        .orderBy(['chromosome', 'position']) \
        .write \
        .mode('overwrite') \
        .csv(outdir, sep='\t', header=False)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
