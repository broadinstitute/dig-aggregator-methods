import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, signum

S3_BUCKET = 's3://dig-giant-sandbox'


def main():
    """
    Arguments:  phenotype
                ancestry - optional, Mixed for trans-ethnic, EU/EA/etc. for ancestry-specific
                dataset - optional, for dataset specific results
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('--phenotype', type=str, required=True)
    opts.add_argument('--ancestry', type=str, required=False)
    opts.add_argument('--dataset', type=str, required=False)

    # parse command line
    args = opts.parse_args()

    if args.ancestry is None and args.dataset is None:
        raise Exception('must set either --ancestry or --dataset')
    if args.ancestry is not None and args.dataset is not None:
        raise Exception('cannot set both --ancestry and --dataset')

    # source data and output location
    if args.ancestry is not None:
        if args.ancestry == 'Mixed':
            srcdir = f'{S3_BUCKET}/out/metaanalysis/trans-ethnic/{args.phenotype}/part-*'
            clumpdir = f'{S3_BUCKET}/out/metaanalysis/staging/clumped/{args.phenotype}/*.json'
            outdir = f'{S3_BUCKET}/out/metaanalysis/clumped/{args.phenotype}'
        else:
            ancestry_path = f'{args.phenotype}/ancestry={args.ancestry}'
            srcdir = f'{S3_BUCKET}/out/metaanalysis/ancestry-specific/{ancestry_path}/part-*'
            clumpdir = f'{S3_BUCKET}/out/metaanalysis/staging/ancestry-clumped/{ancestry_path}/*.json'
            outdir = f'{S3_BUCKET}/out/metaanalysis/ancestry-clumped/{ancestry_path}'
    else:
        dataset_path = f'{args.phenotype}/dataset={args.dataset}'
        srcdir = f'{S3_BUCKET}/variants/GWAS/{args.dataset}/{args.phenotype}/part-*'
        clumpdir = f'{S3_BUCKET}/out/metaanalysis/staging/dataset-clumped/{dataset_path}/*.json'
        outdir = f'{S3_BUCKET}/out/metaanalysis/dataset-clumped/{dataset_path}'

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
