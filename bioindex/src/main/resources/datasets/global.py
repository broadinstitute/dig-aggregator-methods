import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name

# what bucket will be output to?
OUT_BUCKET = f'dig-bio-{"test" if os.getenv("JOB_DRYRUN") else "index"}'


def main():
    """
    Arguments: phenotype
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')

    # parse cmdline
    args = opts.parse_args()

    # load and output directory
    srcdir = f's3://dig-analysis-data/variants/*/{args.phenotype}'
    outdir = f's3://{OUT_BUCKET}/datasets'

    # start the spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load all dataset information
    df = spark.read.json(f'{srcdir}/metadata')

    # keep only specific fields
    df = df.select(
        df.name,
        df.phenotype,
        df.ancestry,
        df.tech,
        df.cases,
        df.controls,
        df.subjects,
    )

    # write out the datasets by phenotype
    df.orderBy(['phenotype']) \
        .repartition(1) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/phenotype')

    # index by name and then phenotype
    df.orderBy(['name', 'phenotype']) \
        .repartition(1) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/name')

    # done
    spark.stop()


if __name__ == '__main__':
    main()
