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
    srcdir = f's3://dig-analysis-data/out/metaanalysis/trans-ethnic/{args.phenotype}/part-*'
    outdir = f's3://dig-bio-index/associations'

    # common vep data
    common_dir = 's3://dig-analysis-data/out/varianteffect/common/part-*'

    # load the trans-ethnic, meta-analysis, top variants and write them sorted
    df = spark.read.json(srcdir)
    common = spark.read.json(common_dir)
    common = common.drop('maf', 'af')

    # join the common data
    df = df.join(common, 'varId', how='left_outer')

    # write associations sorted by locus and then variant id
    df.orderBy(['chromosome', 'position', 'varId']) \
        .write \
        .mode('overwrite') \
        .json('%s/phenotype/%s' % (outdir, args.phenotype))

    # done
    spark.stop()


if __name__ == '__main__':
    main()
