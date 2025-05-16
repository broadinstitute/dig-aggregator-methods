import argparse
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def get_clump_df(spark, clumpdir):
    clump_df = spark.read.json(clumpdir)
    clump_df = clump_df \
        .withColumn('clump', clump_df.credibleSetId) \
        .filter(clump_df.source != 'credible_set')
    # limit the data being written
    clump_df = clump_df.select(
        clump_df.phenotype.alias('leadPhenotype'),
        clump_df.clump,
        clump_df.varId,
        clump_df.alignment,
    )
    return clump_df


def get_assocs_df(spark, assocsdir):
    assocs_df = spark.read.json(assocsdir)
    assocs_df = assocs_df.filter(assocs_df.pValue <= 0.05)
    return assocs_df


def main():
    """
    Arguments:  ancestry - str indicating which ancestry to run the analysis against
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('--ancestry', type=str, required=True)
    args = opts.parse_args()

    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output locations
    common_dir = f'{s3_in}/out/varianteffect/variants/common'
    clumpdir = f'{s3_in}/out/credible_sets/merged/*/{args.ancestry}/part-*'
    if args.ancestry == 'Mixed':
        assocsdir = f'{s3_in}/out/metaanalysis/bottom-line/trans-ethnic/*/part-*'
        outdir = f'{s3_bioindex}/associations/matrix/trans-ethnic'
    else:
        assocsdir = f'{s3_in}/out/metaanalysis/bottom-line/ancestry-specific/*/ancestry={args.ancestry}/part-*'
        outdir = f'{s3_bioindex}/associations/matrix/ancestry/{args.ancestry}'

    clumps = get_clump_df(spark, clumpdir) \
        .withColumn('ancestry', lit(args.ancestry))
    assocs = get_assocs_df(spark, assocsdir)
    common = spark.read.json(f'{common_dir}/part-*') \
        .select('varId', 'dbSNP', 'consequence', 'nearest', 'minorAllele', 'maf', 'af')

    # join to build the associations matrix
    df = clumps.join(assocs, on='varId', how='inner')
    df = df.filter(df.phenotype != df.leadPhenotype)

    # per clump, keep only the best association per phenotype
    df = df.orderBy(['leadPhenotype', 'clump', 'phenotype', 'pValue'])
    df = df.dropDuplicates(['leadPhenotype', 'clump', 'phenotype'])

    # rejoin with the common data
    df = df.join(common, on='varId', how='left_outer')

    # write it out, sorted by the lead phenotype and secondary phenotype
    df.orderBy(['leadPhenotype', 'phenotype']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
