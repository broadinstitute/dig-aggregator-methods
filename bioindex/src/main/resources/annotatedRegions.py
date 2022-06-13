import argparse
import re

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import input_file_name, udf

def main():
    """
    Arguments: sub_region
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('sub_region')

    # extract the dataset from the command line
    args = opts.parse_args()
    partitions = args.sub_region.split('-') if args.sub_region != 'default' else []

    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output directories
    srcdir = f's3://dig-analysis-data/out/ldsc/regions/{args.sub_region}/merged/*/*.csv'
    outdir = 's3://dig-bio-index/regions'

    # input summary stats schema
    schema = StructType([
        StructField('chromosome', StringType(), nullable=False),
        StructField('start', IntegerType(), nullable=False),
        StructField('end', IntegerType(), nullable=False),
        StructField('state', StringType(), nullable=True),
    ])

    # input pathname -> partition
    src_re = r'/out/ldsc/regions/{}/merged/([^/]+)/'.format(args.sub_region)

    # udf functions (NOTE: tissue needs to remove underscores used!)
    annotation = udf(lambda s: re.search(src_re, s).group(1).split('___')[0])
    tissue = udf(lambda s: re.search(src_re, s).group(1).split('___')[1].replace('_', ' ').replace(',', ''))

    def general_udf(idx):
        return udf(lambda s: re.search(src_re, s).group(1).split('___')[idx + 2].replace('_', ' ').replace(',', ''))

    df = spark.read.csv(srcdir, sep='\t', header=False, schema=schema) \
        .withColumn('file_name', input_file_name()) \
        .withColumn('annotation', annotation('file_name')) \
        .withColumn('tissue', tissue('file_name'))
    for i, partition in enumerate(partitions):
        df = df.withColumn(partition, general_udf(i)('file_name'))
    df = df.drop('file_name')

    # sort by annotation and then position
    df.orderBy(['annotation', 'chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/annotation')

    # sort by tissue and then position
    df.orderBy(['tissue', 'chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/tissue')

    # sort by locus
    df.orderBy(['chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/position')

    for partition in partitions:
        # sort by other partitions
        df.orderBy([partition, 'chromosome', 'start']) \
            .write \
            .mode('overwrite') \
            .json(f'{outdir}/{partition}')

    # done
    spark.stop()


if __name__ == '__main__':
    main()
