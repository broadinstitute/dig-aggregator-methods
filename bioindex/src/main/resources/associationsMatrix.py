import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.function import col, rank


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
    srcdir = f's3://dig-analysis-data/out/metaanalysis/trans-ethnic/*/part-*'
    outdir = f's3://{OUT_BUCKET}/associations'

    # common vep data
    common_dir = 's3://dig-analysis-data/out/varianteffect/common'

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load the trans-ethnic, meta-analysis, top variants and write them sorted
    df = spark.read.json(srcdir)
    common = spark.read.json(common_dir)
    w = Window.orderBy('pValue')

    # drop any variant below a p-value threshold
    df = df.filter(df.pValue < 0.05)

    # select only the top 1000 variants for the selected phenotype
    primary = df.filter(df.phenotype == args.phenotype) \
        .withColumn('rank', rank().over(w)) \
        .filter(col('rank') <= 1000) \
        .join(common, 'varId', how='left_outer')

    # remove the primary phenotype from the rest of the data
    df = df.filter(df.phenotype != args.phenotype) \
        .select(df.varId, df.phenotype, df.pValue, df.beta)

    # for every primary variant, get an array of secondary associations
    primary = primary.withColumn()

    # write associations sorted by locus
    primary.write \
        .mode('overwrite') \
        .json('%s/locus/%s' % (outdir, args.phenotype))

    # done
    spark.stop()


if __name__ == '__main__':
    main()
