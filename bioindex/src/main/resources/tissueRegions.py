import re
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import input_file_name, udf

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output directories
    srcdir = f'{s3_in}/out/ldsc/regions/merged/annotation-tissue-biosample/*/*.csv'
    outdir = f'{s3_bioindex}/regions'

    # input summary stats schema
    schema = StructType([
        StructField('chromosome', StringType(), nullable=False),
        StructField('start', IntegerType(), nullable=False),
        StructField('end', IntegerType(), nullable=False),
        StructField('state', StringType(), nullable=True),
        StructField('biosample', StringType(), nullable=True),
        StructField('method', StringType(), nullable=True),
        StructField('source', StringType(), nullable=True),
        StructField('disease', StringType(), nullable=True),
        StructField('dataset', StringType(), nullable=True)
    ])

    # input pathname -> partition
    src_re = r'/out/ldsc/regions/merged/annotation-tissue-biosample/([^/]+)/'

    # udf functions (NOTE: tissue needs to remove underscores used!)
    annotation = udf(lambda s: re.search(src_re, s).group(1).split('___')[0])
    tissue = udf(lambda s: re.search(src_re, s).group(1).split('___')[1])

    df = spark.read.csv(srcdir, sep='\t', header=False, schema=schema) \
        .withColumn('file_name', input_file_name()) \
        .withColumn('annotation', annotation('file_name')) \
        .withColumn('tissue', tissue('file_name'))
    df = df.drop('file_name')

    # sort by tissue and then position
    df.orderBy(['tissue', 'chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/tissue')

    # done
    spark.stop()


if __name__ == '__main__':
    main()
