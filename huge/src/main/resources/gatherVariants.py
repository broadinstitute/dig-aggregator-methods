# imports
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# EC2 development localhost directories
SRCDIR = 's3://dig-analysis-data/out/varianteffect'
OUTDIR = 's3://dig-analysis-data/out/magma'


# CSV schema of the variants input to VEP
VARIANTS_SCHEMA = StructType([
    StructField('chromosome', StringType()),
    StructField('position', IntegerType()),
    StructField('end', IntegerType()),
    StructField('alleles', StringType()),
    StructField('strand', StringType()),
    StructField('varId', StringType()),
])


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('magma').getOrCreate()

    # load all unique variants and snps in the site
    variants = spark.read.csv(f'{SRCDIR}/variants/part-*', sep='\t', schema=VARIANTS_SCHEMA)
    snps = spark.read.csv(f'{SRCDIR}/snp/part-*', sep='\t', header=True)

    # keep only the columns we care about
    variants = variants.select(
        variants.varId,
        variants.chromosome,
        variants.position,
    )

    # drop snps with no rsID
    snps = snps.filter(snps.dbSNP.isNotNull())

    # join them together to get the rsIDs for each variant
    df = variants.join(snps, on='varId')

    # just the dbSNP, chromosome, and position
    df = df.select(
        df.dbSNP,
        df.chromosome,
        df.position,
    )

    # MAGMA requires integer chromosomes instead of X, Y, ...
    df = df.filter(df.chromosome != 'MT') \
        .replace('X', '23', ['chromosome']) \
        .replace('Y', '24', ['chromosome'])

    # write out the final set of variants
    df.write \
        .mode('overwrite') \
        .csv(f'{OUTDIR}/variants', sep='\t', header=False)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
