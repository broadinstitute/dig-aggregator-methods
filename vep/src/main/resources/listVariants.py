#!/usr/bin/python3
import platform

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, length, lit, when  # pylint: disable=E0611

S3_LDSERVER = 's3://dig-analysis-data'
S3DIR_IN = 's3://dig-analysis-data'
S3DIR_OUT = 's3://psmadbec-test'


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
    dataset_srcdir = f'{S3DIR_IN}/variants/*/*/*'
    ld_server_srcdir = f'{S3_LDSERVER}/ld_server/variants/*'
    outdir = f'{S3DIR_OUT}/out/varianteffect/variants'

    # create a spark session
    spark = SparkSession.builder.appName('vep').getOrCreate()

    # slurp all the variants across ALL datasets, but only locus information
    # combine with variants in LD Server to make sure all LD Server variants go through VEP for burden binning
    dataset_df = get_df(spark, dataset_srcdir)
    ld_server_df = get_df(spark, ld_server_srcdir)

    df = dataset_df.union(ld_server_df)\
        .dropDuplicates(['varId'])

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
        .mode('overwrite') \
        .csv(outdir, sep='\t')

    # done
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
