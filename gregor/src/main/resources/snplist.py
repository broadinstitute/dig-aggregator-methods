#!/usr/bin/python3

import argparse
import platform

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, split  # pylint: disable=E0611

S3DIR = 's3://dig-analysis-data'


def main():
    """
    Arguments: <phenotype>
    """
    print('Python version: %s' % platform.python_version())

    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')

    # get command line arguments
    args = opts.parse_args()

    # get the source and output directories
    srcdir = '%s/out/metaanalysis/staging/plink/%s/*/plink.clumped' % (S3DIR, args.phenotype)
    outdir = '%s/out/gregor/snp/%s' % (S3DIR, args.phenotype)

    # start spark
    spark = SparkSession.builder.appName('gregor').getOrCreate()

    # Load all the clumped output together into a single dataframe with ethe
    # ancestry added and parsed from the path.
    #
    # The output of PLINK is whitespace-separated, which Spark can't load.
    # If this was Pandas, we'd load with sep='\s+'. Since the data isn't very
    # large, what we'll do instead is pretend it's a single column by using
    # a tab separator and then split the data ourselves, keeping only the
    # columns we care about.

    df = spark.read.csv(srcdir, sep='\t', header=True) \
        .withColumn('filename', input_file_name()) \
        .withColumn('ancestry', regexp_extract('filename', r'/ancestry=([^/]+)/', 1))

    # get the column for the plink data
    plink = df.columns[0]

    # split the plink column and take just the lead SNP
    snp = split(df[plink], r'\s+').getItem(3).alias('SNP')

    # keep just the lead SNP and the ancestry
    df = df.select(snp, df.ancestry)

    # output the variants as CSV part files for GREGOR
    df.write \
        .mode('overwrite') \
        .partitionBy('ancestry') \
        .csv(outdir, sep='\t')

    # done
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
