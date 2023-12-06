import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

s3_in = 'dig-analysis-hermes'
s3_out = 'dig-analysis-hermes/bioindex'

def get_src_df(spark, srcdir):
    df = spark.read.json(srcdir)
    # limit the data being written
    df = df.select(
        df.varId,
        df.chromosome,
        df.position,
        df.phenotype,
        df.pValue,
        df.beta,
        df.stdErr,
        df.n,
    )
    return df


def get_clump_df(spark, clumpdir):
    clump_df = spark.read.json(clumpdir)
    # limit the data being written
    clump_df = clump_df.select(
        clump_df.varId,
        clump_df.phenotype,
        clump_df.clump
    )
    return clump_df


def main():
    """
    Arguments:  trans-ethnic - flag to indicate analysis for trans-ethnic results
                ancestry-specific - str indicating which ancestry to run the analysis against
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('--ancestry', type=str, required=True)
    args = opts.parse_args()

    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output locations
    s3_bucket = 'dig-bio-index'
    if args.ancestry == 'Mixed':
        srcdir = f's3://{s3_in}/out/metaanalysis/bottom-line/trans-ethnic/*/part-*'
        clumpdir = f's3://{s3_in}/out/metaanalysis/bottom-line/clumped/*/part-*'
        outdir = f's3://{s3_out}/associations/phewas'
    else:
        srcdir = f's3://{s3_out}/out/metaanalysis/bottom-line/ancestry-specific/*/ancestry={args.ancestry}/part-*'
        clumpdir = f's3://{s3_out}/out/metaanalysis/bottom-line/ancestry-clumped/*/ancestry={args.ancestry}/part-*'
        outdir = f's3://{s3_out}/ancestry-associations/phewas/{args.ancestry}'

    df = get_src_df(spark, srcdir) \
        .withColumn('ancestry', lit(args.ancestry))
    clump_df = get_clump_df(spark, clumpdir)

    df = df.join(clump_df, on=['varId', 'phenotype'], how='left')

    # write associations sorted by variant and then p-value
    df.orderBy(['chromosome', 'position', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
