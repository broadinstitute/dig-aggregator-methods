import argparse
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def get_df(spark, bioindex, folder):
    print(bioindex, folder)
    if bioindex == 'dig-bio-index':
        release = 'current'
    else:
        release = re.findall('dig-bio-index-([0-9]{8}).*', bioindex)[0]
    srcdir = f's3://{bioindex}/associations/global/{folder}/part-*'

    df = spark.read.json(srcdir) \
        .withColumn('release', lit(release))

    df = df.filter(df.phenotype.isNotNull())

    return df.groupBy(['phenotype', 'ancestry', 'release', 'inMetaTypes']) \
        .agg(count('varId').alias('count'))


def main():
    """
    Arguments:  ancestry - str indicating which ancestry to run the analysis against
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('--ancestry', type=str)
    opts.add_argument('--bioindices', type=str, required=True)
    args = opts.parse_args()

    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    if args.ancestry == 'TE':
        folder = 'trans-ethnic'
    else:
        folder = f'ancestry/{args.ancestry}'
    outdir = f'{s3_bioindex}/historical/associations/global/{folder}'

    bioindex, other_bioindices = args.bioindices.split(',', 1)
    df = get_df(spark, bioindex, folder)
    for bioindex in other_bioindices.split(','):
        df = df.union(get_df(spark, bioindex, folder))

    df.orderBy(['phenotype', 'ancestry', 'release', col('count').desc()]) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    spark.stop()


if __name__ == '__main__':
    main()
