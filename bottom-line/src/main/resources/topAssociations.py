import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


# what bucket will be output to?
S3_BUCKET = os.getenv('JOB_BUCKET')


def main():
    """
    Arguments: phenotype
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')

    # parse command line
    args = opts.parse_args()

    # source data and output location
    srcdir = f'{S3_BUCKET}/out/metaanalysis/trans-ethnic/{args.phenotype}/part-*'
    clumpdir = f'{S3_BUCKET}/out/metaanalysis/staging/clumped/{args.phenotype}/*.json'
    outdir = f'{S3_BUCKET}/out/metaanalysis/top/{args.phenotype}'

    # initialize spark session
    spark = SparkSession.builder.appName('bottom-line').getOrCreate()

    # load the clumped plink output and all association data
    clumps = spark.read.json(clumpdir)
    assocs = spark.read.json(srcdir)

    # merge with trans-ethnic, association data and sort
    clumps = clumps.join(assocs, ['varId', 'phenotype'])
    clumps = clumps.sort(['clump', 'pValue'])

    # drop duplicate clumps, which leaves only the lead SNPs
    lead_snps = clumps.dropDuplicates(['clump']) \
        .select(col('varId'), lit(True).alias('leadSNP'))

    # join the lead SNPs back with the clumps, set missing as not lead SNPs
    clumps = clumps.join(lead_snps, on='varId', how='left')
    clumps = clumps.na.fill({'leadSNP': False})

    # output lead SNPs
    clumps.write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
