import argparse
import os
import re
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, input_file_name, lit, udf, when

# what bucket will be output to?
S3_BUCKET = os.getenv('JOB_BUCKET')


def s3_test(path):
    """
    Run `aws s3 ls <path> --recursive` to see if any files exist in the path.
    """
    try:
        summaries = subprocess \
            .check_output(['aws', 's3', 'ls', f'{path}/', '--recursive']) \
            .decode('UTF-8') \
            .strip() \
            .split('\n')

        return len(summaries) > 0
    except subprocess.CalledProcessError:
        return False


def main():
    """
    Arguments: none
    """
    srcdir = f'{S3_BUCKET}/out/gregor/summary/*/*/statistics.txt'
    outdir = f'{S3_BUCKET}/out/gregor/enrichment/'

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # input summary stats schema
    schema = StructType(
        [
            StructField('bed', StringType(), nullable=False),
            StructField('SNPs', IntegerType(), nullable=False),
            StructField('expectedSNPs', DoubleType(), nullable=True),
            StructField('pValue', DoubleType(), nullable=True),
        ]
    )

    # input pathname -> phenotype and ancestry
    src_re = r'/out/gregor/summary/([^/]+)/ancestry=([^/]+)/'

    # udf functions (NOTE: tissue needs to remove underscores used!)
    phenotype_of_source = udf(lambda s: s and re.search(src_re, s).group(1))
    ancestry_of_source = udf(lambda s: s and re.search(src_re, s).group(2))
    annotation_of_bed = udf(lambda s: s and s.split('___')[0])
    tissue_of_bed = udf(lambda s: s and s.split('___')[1].replace('_', ' '))

    # load all the summaries; note that ancestry is part of the path
    df = spark.read.csv(srcdir, sep='\t', header=True, schema=schema) \
        .withColumn('source', input_file_name()) \
        .select(
            phenotype_of_source('source').alias('phenotype'),
            ancestry_of_source('source').alias('ancestry'),
            annotation_of_bed('bed').alias('annotation'),
            tissue_of_bed('bed').alias('tissue'),
            col('SNPs'),
            col('expectedSNPs'),
            col('pValue'),
        )

    # remove NA results and write
    df.dropna() \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
