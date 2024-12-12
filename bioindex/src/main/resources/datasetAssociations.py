import argparse
import os

from pyspark.sql import SparkSession

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

    tech, dataset, phenotype, ancestry = args.path.split('/')

    # load and output directory
    common_dir = f'{s3_in}/out/varianteffect/common'
    srcdir = f'{s3_in}/variants/{args.path}/part-*'
    outdir = f'{s3_bioindex}/associations/phenotype/{phenotype}/{ancestry}'

    # load the trans-ethnic, meta-analysis, top variants and write them sorted
    df = spark.read.json(srcdir)

    # write associations sorted by locus, merge into a single file
    df.orderBy(['chromosome', 'position', 'varId']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
