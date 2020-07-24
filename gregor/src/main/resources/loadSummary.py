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
    Arguments: phenotype
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')

    # parse command line
    args = opts.parse_args()

    # data locations
    srcdir = f'{S3_BUCKET}/out/gregor/summary/{args.phenotype}'
    outdir = f'{S3_BUCKET}/out/gregor/enrichment/{args.phenotype}'

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # don't run if there is no summary data for this phenotype
    if s3_test(srcdir):
        statistics_schema = StructType(
            [
                StructField('bed', StringType(), nullable=False),
                StructField('SNPs', IntegerType(), nullable=False),
                StructField('expectedSNPs', DoubleType(), nullable=True),
                StructField('pValue', DoubleType(), nullable=True),
            ]
        )

        # input filename -> phenotype and ancestry
        src_re = r'/out/gregor/summary/([^/]+)/([^/]+)/'

        # udf functions
        phenotype_of_source = udf(lambda s: s and re.search(src_re, s).group(1))
        ancestry_of_source = udf(lambda s: s and re.search(src_re, s).group(2))
        tissue_of_bed = udf(lambda s: s and s.split('___')[0].replace('_', ':'))
        method_of_bed = udf(lambda s: s and s.split('___')[1])
        annotation_of_bed = udf(lambda s: s and s.split('___')[2])

        # load the trans-ethnic, meta-analysis, top variants and write them sorted
        df = spark.read \
            .csv(f'{srcdir}/statistics.txt', sep='\t', header=True, schema=statistics_schema) \
            .select('*', input_file_name().alias('source')) \
            .select(
                phenotype_of_source('source').alias('phenotype'),
                ancestry_of_source('source').alias('ancestry'),
                tissue_of_bed('bed').alias('tissueId'),
                method_of_bed('bed').alias('method'),
                annotation_of_bed('bed').alias('annotation'),
                col('SNPs'),
                col('expectedSNPs'),
                col('pValue'),
            )

        # load the tissue ontology to join
        tissues = spark.read \
            .json(f'{S3_BUCKET}/tissues/ontology') \
            .select(
                col('id').alias('tissueId'),
                col('description').alias('tissue'),
            )

        # convert NA method to null
        method = when(df.method == 'NA', lit(None)).otherwise(df.method)

        # add the method and join the tissue ontology
        df = df.dropna() \
            .withColumn('method', method) \
            .join(tissues, 'tissueId', how='left_outer')

        # write out the summary
        df.write.mode('overwrite').json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
