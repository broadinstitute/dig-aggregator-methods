import argparse
import os

from pyspark.sql import SparkSession

# what bucket will be output to?
S3_BUCKET = os.getenv('JOB_BUCKET')


def main():
    """
    Arguments: phenotype
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')

    # parse command line
    args = opts.parse_args()

    # source data and output location
    srcdir = f'{S3_BUCKET}/out/metaanalysis/trans-ethnic/{args.phenotype}/part-*'
    clumpdir = f'{S3_BUCKET}/out/metaanalysis/clumped/{args.phenotype}/part-*'
    outdir = f'{S3_BUCKET}/out/metaanalysis/top/{args.phenotype}'

    # initialize spark session
    spark = SparkSession.builder.appName('bottom-line').getOrCreate()

    # load the clumped plink output and all association data
    clumps = spark.read.json(clumpdir)
    clumps = clumps.sort(['clump', 'pValue']).dropDuplicates(['clump'])

    # merge with trans-ethnic, association data
    df = spark.read.json(srcdir)

    # remove columns duplicated in clumps from association data
    df = df.drop('chromosome', 'position', 'phenotype', 'pValue')
    df = clumps.join(df, 'varId')

    # write out just the top associations as a single file, sorted by p-value
    df.coalesce(1) \
        .sort(['pValue']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
