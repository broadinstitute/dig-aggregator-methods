import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, signum


def build_clump_matrix(df, phenotype):
    """
    """
    primary = df.filter(df.phenotype == phenotype)

    # get the direction of effect for the lead SNP
    effect = primary.filter(primary.leadSNP).head().beta

    # create the aligned effect multiplier (signum)
    alignment = signum(primary.beta * effect).alias('alignment')

    # keep only the primary variant and its effect multiplier
    primary = primary.select(primary.varId, alignment)

    # find all lead SNPs that link to the primary phenotype clump
    df = df.filter((df.phenotype != phenotype) & (df.leadSNP == True))
    df = df.join(primary, on='varId')

    # add a column indicating what the primary phenotype is
    df = df.withColumn('leadPhenotype', lit(phenotype))

    return df


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load and output directory
    srcdir = 's3://dig-analysis-data/out/metaanalysis'
    common_dir = 's3://dig-analysis-data/out/varianteffect/common'
    outdir = 's3://dig-bio-index/associations/clumped'

    # load the clumps
    clumps = spark.read.json(f'{srcdir}/clumped/*/part-*')
    clumps = clumps.select(
        clumps.varId,
        clumps.phenotype,
        clumps.clump,
        clumps.pValue,
    )

    # sort and identify the lead SNPs
    clumps = clumps.sort(['phenotype', 'clump', 'pValue'])
    lead_snps = clumps.dropDuplicates(['phenotype', 'clump']) \
        .withColumn('leadSNP', lit(True)) \
        .select('varId', 'leadSNP')

    # now join, so the clumps frame has a leadSNP column
    clumps = clumps.join(lead_snps, on='varId', how='left')
    clumps = clumps.na.fill({'leadSNP': False})

    # drop the p-value from the clump frame, it'll get added back later
    clumps = clumps.drop('pValue')

    # read all the bottom-line associations and common data
    assoc = spark.read.json(f'{srcdir}/trans-ethnic/*/part-*')
    common = spark.read.json(f'{common_dir}/part-*')

    # join to get all the association and common data for all clumped SNPs
    df = clumps.join(assoc, on='varId', how='inner')
    df = clumps.join(common, on='varId', how='left_outer')

    # write out all the clumped associations
    df.orderBy(['phenotype', 'clump', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # get all the unique phenotypes as a set
    phenotypes = [r.phenotype for r in df.dropDuplicates(df.phenotype).collect()]

    # for each phenotype, build a clump matrix
    for phenotype in phenotypes:
        build_clump_matrix(df, phenotype)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
