#!/usr/bin/python3
import argparse
import boto3
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, length, lit, when  # pylint: disable=E0611

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def get_df(spark, srcdir):
    return spark.read.json(f'{srcdir}/part-*') \
        .select(
        'varId',
        'chromosome',
        'position',
        'reference',
        'alt',
    )


def check_path(path):
    bucket, non_bucket_path = re.findall('s3://([^/]*)/(.*)', path)[0]
    s3 = boto3.client('s3')
    return s3.list_objects_v2(Bucket=bucket, Prefix=non_bucket_path)['KeyCount'] > 0


def get_src(data_type):
    if data_type == 'variants':
        return f'{s3_in}/variants/*/*/*'
    elif data_type == 'ld_server':
        return f'{s3_in}/ld_server/variants/*'
    elif data_type == 'variant_counts':
        return f'{s3_in}/variant_counts/*/*/*'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-type', type=str, required=True, help="Data type to process")
    args = parser.parse_args()

    # get the source and output directories
    srcdir = get_src(args.data_type)
    outdir = f'{s3_out}/out/varianteffect/{args.data_type}/variants'

    # create a spark session
    spark = SparkSession.builder.appName('vep').getOrCreate()

    # slurp all the variants across ALL datasets, but only locus information
    # combine with variants in LD Server to make sure all LD Server variants go through VEP for burden binning
    df = get_df(spark, srcdir)

    # varIds with length > 1000 causes VEP to hang
    df = df.dropDuplicates(['varId']) \
        .filter(length(df.varId) < 1000)

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
    files = df.count() // 100000
    if files < 1000:
        df.repartition(files).write \
            .mode('overwrite') \
            .csv(outdir, sep='\t')
    else:
        df.write \
            .mode('overwrite') \
            .csv(outdir, sep='\t')

    # done
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
