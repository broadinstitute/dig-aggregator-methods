import argparse

from pyspark.sql import SparkSession

s3_in = 'dig-giant-sandbox'
s3_bucket = 'dig-giant-sandbox/bioindex'

def main():
    """
    Arguments:  ancestry - str indicating which ancestry to run the analysis against
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('--ancestry', type=str, required=True)
    args = opts.parse_args()

    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output locations
    if args.ancestry == 'Mixed':
        srcdir = f's3://{s3_in}/out/metaanalysis/dataset-clumped/*/*/part-*'
        outdir = f's3://{s3_bucket}/associations/{{}}'
    else:
        srcdir = f's3://{s3_in}/out/metaanalysis/ancestry-clumped/*/ancestry={args.ancestry}/part-*'
        outdir = f's3://{s3_bucket}/ancestry-associations/{{}}/{args.ancestry}'

    df = spark.read.json(srcdir)

    # common vep data
    common_dir = 's3://dig-analysis-data/out/varianteffect/common/part-*'

    # load the top-association, lead SNPs for every phenotype
    df = df.filter(df.leadSNP)

    # load common data for variants and join
    common = spark.read.json(common_dir)
    df = df.join(common, on='varId', how='left')

    # sort all by clump range
    df.orderBy(['ancestry', 'sex', 'chromosome', 'clumpStart']) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('top'))

    # sort by phenotype then p-value for global associations
    df.orderBy(['ancestry', 'sex', 'phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('global'))

    # done
    spark.stop()


if __name__ == '__main__':
    main()
