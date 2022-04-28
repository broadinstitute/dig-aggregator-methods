#!/usr/bin/python3

import argparse
import platform

from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, lit, when  # pylint: disable=E0611

var_dir = 's3://dig-analysis-data'
s3dir = 's3://psmadbec-test'

# entry point
if __name__ == '__main__':
    """
    @param phenotype e.g. `T2D`
    """
    print('Python version: %s' % platform.python_version())

    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')

    # parse the command line parameters
    args = opts.parse_args()

    # get the source and output directories
    srcdir = '%s/variants/*/*/%s' % (var_dir, args.phenotype)
    outdir = '%s/out/metaanalysis/variants/%s' % (s3dir, args.phenotype)

    # create a spark session
    spark = SparkSession.builder.appName('bottom-line').getOrCreate()

    # slurp all the variant batches
    df = spark.read.json('%s/part-*' % srcdir)

    # if ancestry isn't set assume it's mixed
    ancestry = when(df.ancestry.isNull(), lit('Mixed')) \
        .otherwise(df.ancestry)

    # # keep a sum total across datasets for variants with EAF and/or MAF
    # eafCount = when(df.eaf.isNull() | isnan(df.eaf), 0).otherwise(1)
    # mafCount = when(df.maf.isNull() | isnan(df.maf), 0).otherwise(1)

    # # EAF and MAF need to be NA if not preset or NaN so that METAL ignores them
    # eaf = when(eafCount == 1, df.eaf).otherwise(lit('NA'))
    # maf = when(mafCount == 1, df.maf).otherwise(lit('NA'))

    # rare variants have an allele frequency < 5%
    rare = when(df.maf.isNotNull() & (df.maf < 0.05), lit(True)).otherwise(lit(False))

    # keep only variants for the desired phenotype, that are bi-allelic, and
    # have a valid p-value
    df = df \
        .filter(df.phenotype == args.phenotype) \
        .filter(df.multiAllelic == False) \
        .filter(df.pValue.isNotNull() & ~isnan(df.pValue)) \
        .filter(df.beta.isNotNull() & ~isnan(df.beta)) \
        .filter(df.n.isNotNull()) \
        .select(
            df.dataset,
            df.varId,
            df.chromosome,
            df.position,
            df.reference,
            df.alt,
            df.phenotype,
            ancestry.alias('ancestry'),
            df.pValue,
            df.beta,
            df.stdErr,
            df.n,
            rare.alias('rare'),
        )

    # output the partitioned variants as CSV files for METAL
    df.write \
        .mode('overwrite') \
        .partitionBy('dataset', 'ancestry', 'rare') \
        .csv(outdir, sep='\t', header=True)

    # done
    spark.stop()
