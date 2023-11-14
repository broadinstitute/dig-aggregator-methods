import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, signum

S3_BUCKET = 's3://dig-analysis-data'


def main():
    """
    Arguments:  phenotype
                trans-ethnic - flag to indicate analysis for trans-ethnic results
                ancestry - str indicating which ancestry to run the analysis against
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('--phenotype', type=str, required=True)
    opts.add_argument('--ancestry', type=str, required=True)

    # parse command line
    args = opts.parse_args()

    # source data and output location
    if args.ancestry == 'Mixed':
        srcdir = f'{S3_BUCKET}/out/metaanalysis/bottom-line/trans-ethnic/{args.phenotype}/part-*'
        clumpdir = f'{S3_BUCKET}/out/metaanalysis/bottom-line/staging/clumped/{args.phenotype}/*.json'
        outdir = f'{S3_BUCKET}/out/metaanalysis/bottom-line/clumped/{args.phenotype}'
    else:
        ancestry_path = f'{args.phenotype}/ancestry={args.ancestry}'
        srcdir = f'{S3_BUCKET}/out/metaanalysis/bottom-line/ancestry-specific/{ancestry_path}/part-*'
        clumpdir = f'{S3_BUCKET}/out/metaanalysis/bottom-line/staging/ancestry-clumped/{ancestry_path}/*.json'
        outdir = f'{S3_BUCKET}/out/metaanalysis/bottom-line/ancestry-clumped/{ancestry_path}'

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

    # get the lead SNPs again, this time with the beta
    lead_snps = clumps.filter(clumps.leadSNP == True) \
        .select(clumps.clump, clumps.beta.alias('alignment'))

    # calculate the alignment direction for each of the SNPs in the clumps
    clumps = clumps.join(lead_snps, on='clump')
    clumps = clumps.withColumn('alignment', signum(clumps.beta * clumps.alignment))

    # output lead SNPs
    clumps.write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
