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
    srcdir = f's3://dig-analysis-data/variants/*/{args.phenotype}/part-*'
    outdir = f's3://dig-bio-index/associations/variant'

    # load only the data needed across datasets for a phenotype
    df = spark.read.json(srcdir)
    df = df.select(
        df.varId,
        df.dataset,
        df.phenotype,
        df.pValue,
        df.beta,
        df.n,
    )

    # write associations sorted by phenotype, varId, then pValue
    df.orderBy(['varId', 'phenotype']) \
        .write \
        .mode('overwrite') \
        .json('%s/%s' % (outdir, args.phenotype))

    # done
    spark.stop()


if __name__ == '__main__':
    main()
