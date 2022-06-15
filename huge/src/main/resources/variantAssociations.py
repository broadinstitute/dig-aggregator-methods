import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType


def main():
    """
    Arguments: phenotype
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')

    # parse command line
    args = opts.parse_args()

    # input and output directories
    s3dir = f's3://dig-analysis-data/out'
    outdir = f'{s3dir}/magma/variant-associations/{args.phenotype}'

    # start spark
    spark = SparkSession.builder.appName('magma').getOrCreate()

    # load variants and phenotype associations
    df = spark.read.json(f'{s3dir}/metaanalysis/trans-ethnic/{args.phenotype}/part-*')
    snps = spark.read.csv(f'{s3dir}/varianteffect/snp/part-*', sep='\t', header=True)

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
