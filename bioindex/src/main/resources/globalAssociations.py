import argparse

from pyspark.sql import SparkSession


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
    srcdir = f's3://dig-analysis-data/out/metaanalysis/trans-ethnic'
    outdir = f's3://dig-bio-index/associations/global'

    # common vep data
    common_dir = 's3://dig-analysis-data/out/varianteffect/common'

    # load all trans-ethnic, meta-analysis results for all variants
    df = spark.read.json(f'{srcdir}/*/part-*')
    common = spark.read.json(f'{common_dir}/part-*')

    # get significant associations for the phenotype
    df = df.filter(df.phenotype == args.phenotype)
    df = df.filter(df.pValue < 5e-8)

    # join with common
    df = df.join(common, 'varId', how='left_outer')

    # coalesce, sort by p-value and write
    df.coalesce(1) \
        .orderBy(['pValue']) \
        .write \
        .mode('overwrite') \
        .json('%s/%s' % (outdir, args.phenotype))

    # done
    spark.stop()


if __name__ == '__main__':
    main()
