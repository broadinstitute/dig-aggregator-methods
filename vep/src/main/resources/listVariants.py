#!/usr/bin/python3
import platform

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, length, lit, when  # pylint: disable=E0611

S3DIR = 's3://dig-analysis-data'


def get_df(spark, srcdir):
    return spark.read.json(f'{srcdir}/part-*') \
        .select(
        'varId',
        'chromosome',
        'position',
        'reference',
        'alt',
    )


def main():
    """
    Arguments: none
    """
    print('Python version: %s' % platform.python_version())

    # get the source and output directories
    dataset_srcdir = f'{S3DIR}/variants/GWAS/Agrawal2022_LocalAdiposity_Mixed_males/ASAT'
    ld_server_srcdir = f'{S3DIR}/ld_server/variants/*'
    outdir = 's3://drew-vep-test/out/varianteffect/variants'
    existing_variant_data = f'{S3DIR}/out/varianteffect/variants'

    # create a spark session
    spark = SparkSession.builder.appName('vep').getOrCreate()

    # slurp all the variants across ALL datasets, but only locus information
    # combine with variants in LD Server to make sure all LD Server variants go through VEP for burden binning
    dataset_df = get_df(spark, dataset_srcdir)
    # ld_server_df = get_df(spark, ld_server_srcdir)
    existing_variants = spark.read.csv(existing_variant_data, sep='\t', header=False)
    just_existing_variants = existing_variants.select('_c5')
    just_existing_variants = just_existing_variants.withColumnRenamed('_c5', 'varId')
    df = dataset_df.join(just_existing_variants, dataset_df.varId == just_existing_variants.varId,
                    "leftanti")

    # get the length of the reference and alternate alleles
    ref_len = length(df.reference)
    alt_len = length(df.alt)

    # Calculate the end position from the start and whether there was an
    # insertion or deletion.
    #
    # See: https://useast.ensembl.org/info/docs/tools/vep/vep_formats.html
    #
    end = when(ref_len == 0, df.position + alt_len - 1) \
        .otherwise(df.position + ref_len - 1) \
        .cast('int')

    # check for insertion
    start = when(ref_len == 0, end + 1) \
        .otherwise(df.position) \
        .cast('int')

    # join the reference and alternate alleles together
    allele = concat_ws('/', df.reference, df.alt)
    strand = lit('+')

    # extract only the fields necessary for the VCF file
    df = df.select(
        df.chromosome,
        start,
        end,
        allele,
        strand,
        df.varId,
    )

    # output the variants as CSV part files
    df.write \
        .mode('append') \
        .csv(outdir, sep='\t')

    # done
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
