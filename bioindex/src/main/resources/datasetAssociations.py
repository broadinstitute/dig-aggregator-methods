import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import rank
from pyspark.sql.window import Window

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    """
    Arguments: dataset/phenotype
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('path')

    # parse command line
    args = opts.parse_args()

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load and output directory
    common_dir = f'{s3_in}/out/varianteffect/common'
    srcdir = f'{s3_in}/variants/{args.path}/part-*'
    outdir = f'{s3_bioindex}/associations/dataset'

    # load the trans-ethnic, meta-analysis, top variants and write them sorted
    df = spark.read.json(srcdir)
    common = spark.read.json(common_dir) \
        .select('varId', 'dbSNP', 'consequence', 'nearest', 'minorAllele', 'maf', 'af')

    # rank the variants by p-value, keep only the top 1500
    w = Window().orderBy('pValue')

    # keep just the top variants per dataset
    df = df.withColumn('rank', rank().over(w))
    df = df.filter(df.rank <= 1500)
    df = df.filter((df.pValue <= 0.05) | (df.rank <= 500))

    # join common variant data last
    df = df.join(common, 'varId', how='left_outer')

    # write associations sorted by locus, merge into a single file
    df.drop('rank') \
        .orderBy(['pValue']) \
        .write \
        .mode('overwrite') \
        .json('%s/%s' % (outdir, args.path))

    # done
    spark.stop()


if __name__ == '__main__':
    main()
