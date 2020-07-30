import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import rank
from pyspark.sql.window import Window


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
    srcdir = f's3://dig-analysis-data/variants/{args.path}/part-*'
    outdir = f's3://dig-bio-index/associations/dataset'

    # common vep data
    common_dir = 's3://dig-analysis-data/out/varianteffect/common'

    # load the trans-ethnic, meta-analysis, top variants and write them sorted
    df = spark.read.json(srcdir)
    common = spark.read.json(common_dir)

    # join the common data
    df = df.join(common, 'varId', how='left_outer')

    # order by p-value
    w = Window().orderBy('pValue')

    # keep just the top 1000 variants per dataset
    df = df.withColumn('rank', rank().over(w))
    df = df.filter(df.rank <= 1000)

    # write associations sorted by locus, merge into a single file
    df.drop('rank') \
        .coalesce(1) \
        .orderBy(['pValue']) \
        .write \
        .mode('overwrite') \
        .json('%s/%s' % (outdir, args.path))

    # done
    spark.stop()


if __name__ == '__main__':
    main()
