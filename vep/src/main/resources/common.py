#!/usr/bin/python3

import argparse
import os.path
import re

from pyspark.sql import SparkSession, Row

S3DIR = 's3://dig-analysis-data/out/varianteffect'


def common_fields(row):
    """
    Extracts data from the colocated variant fields.
    """
    variant = next((v for v in row.colocated_variants or [] if v.allele_string == row.allele_string), None)

    # no colocated variant found, just return the common data
    if not variant:
        return Row(
            varId=row.id,
            consequence=row.most_severe_consequence,
            nearest=row.nearest,
            dbSNP=None,
            maf=None,
            af=None,
        )

    # get the allele for frequency data
    allele = row.id.split(':')[-1]

    return Row(
        varId=row.id,
        consequence=row.most_severe_consequence,
        nearest=row.nearest,
        dbSNP=variant.id if variant.id.startswith('rs') else None,
        maf=variant.minor_allele_freq,
        af=variant.frequencies and Row(
            eur=variant.frequencies[allele].eur,
            amr=variant.frequencies[allele].amr,
            afr=variant.frequencies[allele].afr,
            eas=variant.frequencies[allele].eas,
            sas=variant.frequencies[allele].sas,
        ),
    )


def main():
    """
    Arguments: part-file
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('part')

    # parse cli
    args = opts.parse_args()

    # separate the part filename from the source
    _, filename = os.path.split(args.part)

    # get just the base part
    outfile = re.match(r'^(part-\d+).*', filename).group(1)

    # where to write the output to
    srcdir = f'{S3DIR}/effects'
    outdir = f'{S3DIR}/common'

    # initialize spark
    spark = SparkSession.builder.appName('vep').getOrCreate()

    # load effect data
    df = spark.read.json(f'{srcdir}/{args.part}')

    # extract just the common fields and write them out
    df.rdd \
        .map(common_fields) \
        .toDF() \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/{outfile}')

    # done
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
