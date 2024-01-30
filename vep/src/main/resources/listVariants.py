#!/usr/bin/python3
import platform

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, length, lit, when, broadcast  # pylint: disable=E0611

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

    # just specify, potentially with a list only new datasets
    dataset_srcdir = f'{S3DIR}/variants/ExSeq/*/*'
    existing_variants = f'{S3DIR}/out/varianteffect/variants'
    new_variants = f'{S3DIR}/out/varianteffect/new-variants'


    spark = SparkSession.builder.appName('vep').getOrCreate()

    # reduce new datasets just to their variants
    dataset_df = get_df(spark, dataset_srcdir)
    df = dataset_df.dropDuplicates(['varId'])

    # read in all the variants we already know about and put them in varId column
    existing_variants_df = spark.read.csv(existing_variants, sep='\t', header=False)
    existing_variants_df = existing_variants_df.select('_c5').withColumnRenamed('_c5', 'varId')

    # find variants in new datasets that aren't already in existing datasets, this code
    # assumes that the data in df is significantly smaller than existing_variants_df
    df = broadcast(df).join(existing_variants_df, dataset_df.varId == existing_variants.varId,
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

    # if we find new variants, put them in the new_variants path for additional processing
    # and append them to the existing deduped variants path
    if not df.rdd.isEmpty():
        df.write.mode('overwrite').csv(new_variants, sep='\t')
        df.write.mode('append').csv(existing_variants, sep='\t')

    # done
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
