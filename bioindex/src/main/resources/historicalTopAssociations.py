import argparse
import os
import re
import boto3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def check_path(path):
    bucket, non_bucket_path = re.findall('s3://([^/]*)/(.*)', path)[0]
    s3 = boto3.client('s3')
    return s3.list_objects_v2(Bucket=bucket, Prefix=non_bucket_path)['KeyCount'] > 0


def get_df(spark, bioindex, folder):
    print(bioindex, folder)
    if bioindex == 'current':
        release = 'current'
        srcdir = f'{s3_bioindex}/associations/global/{folder}'
    else:
        release = bioindex[:8]
        srcdir = f's3://dig-bio-index-historical/{bioindex}/associations/global/{folder}'

    if check_path(srcdir):
        df = spark.read.json(f'{srcdir}/part-*') \
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

    df = None
    for bioindex in args.bioindices.split(','):
        if df is None:
            df = get_df(spark, bioindex, folder)
        else:
            df = df.union(get_df(spark, bioindex, folder))

    df.orderBy(['phenotype', 'ancestry', 'release', col('count').desc()]) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    spark.stop()


if __name__ == '__main__':
    main()
