import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    """
    Arguments: phenotype
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('--phenotype', type=str, required=True)
    opts.add_argument('--ancestry', type=str, required=True)

    # parse command line
    args = opts.parse_args()

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load the trans-ethnic, meta-analysis, top variants and write them sorted
    common_dir = f'{s3_in}/out/varianteffect/common/part-*'
    if args.ancestry == 'Mixed':
        srcdir = f'{s3_in}/out/metaanalysis/bottom-line/trans-ethnic/{args.phenotype}/part-*'
        outdir = f'{s3_bioindex}/associations/phenotype/trans-ethnic/{args.phenotype}'
    else:
        srcdir = f'{s3_in}/out/metaanalysis/bottom-line/ancestry-specific/{args.phenotype}/ancestry={args.ancestry}/part-*'
        outdir = f'{s3_bioindex}/ancestry-associations/phenotype/{args.phenotype}/{args.ancestry}'

    df = spark.read.json(srcdir) \
        .withColumn('ancestry', lit(args.ancestry))

    # common vep data (can we cache this?)
    common = spark.read.json(common_dir) \
        .select('varId', 'dbSNP', 'consequence', 'nearest')

    # join the common data
    df = df.join(common, 'varId', how='left_outer')

    # write associations sorted by locus and then variant id
    df.orderBy(['chromosome', 'position', 'varId']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
