import argparse
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, signum

S3_BUCKET = 's3://dig-analysis-data'

def check_clump_error(clump_path):
    return subprocess.call(['aws', 's3', 'ls', clump_path, '--recursive'])


def main():
    """
    Arguments:  phenotype
                trans-ethnic - flag to indicate analysis for trans-ethnic results
                ancestry - str indicating which ancestry to run the analysis against
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('--phenotype', type=str, required=True)
    opts.add_argument('--ancestry', type=str, required=True)
    opts.add_argument('--meta-type', type=str, required=True)
    opts.add_argument('--param-type', type=str, required=True)

    # parse command line
    args = opts.parse_args()

    # source data and output location
    if args.ancestry == 'TE':
        srcdir = f'{S3_BUCKET}/out/metaanalysis/{args.meta_type}/trans-ethnic/{args.phenotype}/part-*'
        clumpdir = f'{S3_BUCKET}/out/metaanalysis/{args.meta_type}/staging/clumped/{args.phenotype}'
        outdir = f'{S3_BUCKET}/out/metaanalysis/{args.meta_type}/clumped/{args.phenotype}'
    else:
        param_type_suffix = '-analysis' if args.param_type == 'analysis' else ''
        ancestry_path = f'{param_type_suffix}/{args.phenotype}/ancestry={args.ancestry}'
        srcdir = f'{S3_BUCKET}/out/metaanalysis/{args.meta_type}/ancestry-specific/{args.phenotype}/ancestry={args.ancestry}/part-*'
        clumpdir = f'{S3_BUCKET}/out/metaanalysis/{args.meta_type}/staging/ancestry-clumped{ancestry_path}'
        outdir = f'{S3_BUCKET}/out/metaanalysis/{args.meta_type}/ancestry-clumped{ancestry_path}'

    # initialize spark session
    spark = SparkSession.builder.appName('bottom-line').getOrCreate()

    # load the clumped plink output and all association data
    if not check_clump_error(clumpdir):
        clumps = spark.read.json(f'{clumpdir}/*.json')
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
