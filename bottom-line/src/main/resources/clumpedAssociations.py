import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, signum

# what bucket will be output to?
S3_BUCKET = os.getenv('JOB_BUCKET')


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
        srcdir = f'{S3_BUCKET}/out/metaanalysis/trans-ethnic/{args.phenotype}/part-*'
        clumpdir = f'{S3_BUCKET}/out/metaanalysis/staging/clumped/{args.phenotype}/*.json'
        outdir = f'{S3_BUCKET}/out/metaanalysis/clumped/{args.phenotype}'
    else:
        ancestry_path = f'{args.phenotype}/ancestry={args.ancestry}'
        srcdir = f'{S3_BUCKET}/out/metaanalysis/ancestry-specific/{ancestry_path}/part-*'
        clumpdir = f'{S3_BUCKET}/out/metaanalysis/staging/ancestry-clumped/{ancestry_path}/*.json'
        outdir = f'{S3_BUCKET}/out/metaanalysis/ancestry-clumped/{ancestry_path}'

    # initialize spark session
    spark = SparkSession.builder.appName('bottom-line').getOrCreate()

    # load the clumped plink output and all association data
    clumps = spark.read.json(clumpdir)
    clumps[clumps.varId == '17:42449025:C:G'].show()
    assocs = spark.read.json(srcdir)
    assocs[assocs.varId == '17:42449025:C:G'].show()

    # merge with trans-ethnic, association data and sort
    clumps = clumps.join(assocs, ['varId', 'phenotype'])
    clumps = clumps.sort(['clump', 'pValue'])
    clumps[clumps.varId == '17:42449025:C:G'].show()

    # drop duplicate clumps, which leaves only the lead SNPs
    lead_snps = clumps.dropDuplicates(['clump']) \
        .select(col('varId'), lit(True).alias('leadSNP'))
    lead_snps[lead_snps.varId == '17:42449025:C:G'].show()

    # join the lead SNPs back with the clumps, set missing as not lead SNPs
    clumps = clumps.join(lead_snps, on='varId', how='left')
    clumps = clumps.na.fill({'leadSNP': False})
    clumps[clumps.varId == '17:42449025:C:G'].show()

    # get the lead SNPs again, this time with the beta
    lead_snps = clumps.filter(clumps.leadSNP == True) \
        .select(clumps.clump, clumps.beta.alias('alignment'))
    lead_snps[lead_snps.clump == 27].show()

    # calculate the alignment direction for each of the SNPs in the clumps
    clumps = clumps.join(lead_snps, on='clump')
    clumps = clumps.withColumn('alignment', signum(clumps.beta * clumps.alignment))
    clumps[clumps.varId == '17:42449025:C:G'].show()

    # output lead SNPs
    clumps.write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
