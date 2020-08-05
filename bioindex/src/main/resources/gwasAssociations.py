import argparse

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank


def main():
    """
    Arguments: phenotype
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')

    # parse command line
    args = opts.parse_args()

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load and output directory
    srcdir = f's3://dig-analysis-data/out/metaanalysis/trans-ethnic/{args.phenotype}/'
    outdir = f's3://dig-bio-index/associations/gwas'

    # common vep data
    common_dir = 's3://dig-analysis-data/out/varianteffect/common'

    # load all trans-ethnic, meta-analysis results for all variants
    df = spark.read.json(f'{srcdir}/part-*')
    common = spark.read.json(f'{common_dir}/part-*')

    # find the most significant variants for this phenotype
    w = Window().orderBy('pValue')
    df = df.withColumn('rank', rank().over(w))
    df = df.filter((df.pValue < 1e-5) | (df.rank <= 5000)) \
        .drop('rank')

    # write associations sorted by variant and then p-value
    df.join(common, 'varId', how='left_outer') \
        .coalesce(1) \
        .orderBy(['pValue']) \
        .write \
        .mode('overwrite') \
        .json('%s/%s' % (outdir, args.phenotype))

    # done
    spark.stop()


if __name__ == '__main__':
    main()
