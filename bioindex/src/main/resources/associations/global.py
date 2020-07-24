import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, IntegerType

# what bucket will be output to?
OUT_BUCKET = f'dig-bio-{"test" if os.getenv("JOB_DRYRUN") else "index"}'


def main():
    """
    Arguments: phenotype
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')

    # parse command line
    args = opts.parse_args()

    # load and output directory
    srcdir = f's3://dig-analysis-data/out/metaanalysis/trans-ethnic/{args.phenotype}/part-*'
    outdir = f's3://{OUT_BUCKET}/associations'

    # common vep data
    common_dir = 's3://dig-analysis-data/out/varianteffect/common'

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load the trans-ethnic, meta-analysis, top variants and write them sorted
    df = spark.read.json(srcdir)
    common = spark.read.json(common_dir)

    # join the common data
    df = df.join(common, 'varId', how='left_outer')

    # write associations sorted by locus
    df.orderBy(['chromosome', 'position']) \
        .write \
        .mode('overwrite') \
        .json('%s/locus/%s' % (outdir, args.phenotype))

    # done
    spark.stop()


if __name__ == '__main__':
    main()
