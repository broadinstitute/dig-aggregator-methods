import argparse
import os
from pyspark.sql import SparkSession

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']

def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--project', type=str, required=True)

    # parse command line
    args = opts.parse_args()
    project = 'eQTL' if 'eQTL' in args.project else 'sQTL'

    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = f'{s3_in}/annotated_regions/genetic_variant_effects/{args.project}'
    outdir = f'{s3_bioindex}/regions/variant_links/{project}'

    df = spark.read.json(f'{srcdir}/*.json.zst')

    # sort and write
    df.orderBy(['tissue', 'chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(outdir + '/tissue')

    # sort and write
    df.orderBy(['gene']) \
        .write \
        .mode('overwrite') \
        .json(outdir + '/gene')

    # sort and write
    df.orderBy(['chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(outdir + '/region')

    # done
    spark.stop()


if __name__ == '__main__':
    main()
