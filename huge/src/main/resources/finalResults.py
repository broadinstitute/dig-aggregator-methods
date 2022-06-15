# imports
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import lit, split


NCBI_SRC = 's3://dig-analysis-data/bin/magma/NCBI37.3.gene.loc'
NCBI_SCHEMA = StructType() \
    .add("geneNcbiId", IntegerType(), True) \
    .add("chromosome", StringType(), True) \
    .add("start", IntegerType(), True) \
    .add("end", IntegerType(), True) \
    .add("direction", StringType(), True) \
    .add("gene", StringType(), True)


def main():
    """
    Arguments: phenotype
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')

    # parse command line
    args = opts.parse_args()

    # start spark session
    spark = SparkSession.builder.appName('magma').getOrCreate()

    # EC2 development localhost directories
    srcfile = f's3://dig-analysis-data/out/magma/staging/genes/{args.phenotype}/associations.genes.out'
    outdir = f's3://dig-analysis-data/out/magma/gene-associations/{args.phenotype}'

    # read the NCBI data
    ncbi = spark.read.csv(NCBI_SRC, sep='\t', header=False, schema=NCBI_SCHEMA)

    # NOTE: This file is whitespace-delimited, which Spark can't properly read.
    #       For this reason, we load it with a dummy delimiter and only get a
    #       single column back. We then split the columns and cast them to the
    #       correct type.

    df = spark.read.csv(srcfile, sep='^', header=True)
    df = df.select(split(df[0], r'\s+'))
    df = df.select(
        df[0][0].alias('geneNcbiId').cast(IntegerType()),
        df[0][5].alias('nParam').cast(IntegerType()),
        df[0][6].alias('subjects').cast(IntegerType()),
        df[0][7].alias('zStat').cast(DoubleType()),
        df[0][8].alias('pValue').cast(DoubleType()),
        lit(args.phenotype).alias('phenotype'),
    )

    # join the NCBI data with the gene output
    df = df.join(ncbi, on='geneNcbiId')
    df = df.select(
        df.gene,
        lit(args.phenotype).alias('phenotype'),
        df.nParam,
        df.subjects,
        df.zStat,
        df.pValue,
    )

    # write the results
    df.write.mode('overwrite').json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
