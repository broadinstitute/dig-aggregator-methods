#!/usr/bin/python3
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

s3dir = f's3://kmoogala-test'


def get_s3_dirs(phenotype, ancestry, dataset):
    if ancestry is None:
        return f'{s3dir}/variants/GWAS/{dataset}/{phenotype}/', \
               f'{s3dir}/out/magma/variant-associations/{phenotype}/dataset={dataset}'
    else:
        return f'{s3dir}/out/metaanalysis/ancestry-specific/{phenotype}/ancestry={ancestry}/', \
               f'{s3dir}/out/magma/variant-associations/{phenotype}/ancestry={ancestry}'


def main():
    """
     Arguments:  phenotype
                 ancestry
     """
    opts = argparse.ArgumentParser()
    opts.add_argument('--phenotype', type=str, required=True)
    opts.add_argument('--ancestry', type=str, required=False)
    opts.add_argument('--dataset', type=str, required=False)

    # parse command line
    args = opts.parse_args()

    if args.ancestry is None and args.dataset is None:
        raise Exception('Must define either --ancestry or --dataset flag')
    if args.ancestry is not None and args.dataset is not None:
        raise Exception('Cannot define both --ancestry and --dataset flag')

    # start spark
    spark = SparkSession.builder.appName('magma').getOrCreate()

    # input and output directories
    snpdir = f'{s3dir}/out/varianteffect/snp'
    srcdir, outdir = get_s3_dirs(args.phenotype, args.ancestry, args.dataset)
    print(f'Using directory: {srcdir}')

    # load variants and phenotype associations
    df = spark.read.json(f'{srcdir}/part-*')
    snps = spark.read.csv(f'{snpdir}/part-*', sep='\t', header=True)

    # drop variants with no dbSNP and join
    snps = snps.filter(snps.dbSNP.isNotNull())

    # join to get the rsID for each
    df = df.join(snps, on='varId')

    # keep only the columns magma needs in the correct order
    df = df.select(
        df.dbSNP.alias('SNP'),
        df.pValue.alias('P'),
        df.n.cast(IntegerType()).alias('subjects'),
    )

    # output results
    df.write \
        .mode('overwrite') \
        .csv(outdir, sep='\t', header='true')

    # done
    spark.stop()


if __name__ == '__main__':
    main()
