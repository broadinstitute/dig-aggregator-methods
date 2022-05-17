import re

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import input_file_name, udf

def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output directories
    srcdir = 's3://dig-analysis-data/out/ldsc/regions/merged/*/*.csv'
    outdir = 's3://dig-bio-index/regions'

    # input summary stats schema
    schema = StructType([
        StructField('chromosome', StringType(), nullable=False),
        StructField('start', IntegerType(), nullable=False),
        StructField('end', IntegerType(), nullable=False),
        StructField('state', StringType(), nullable=True),
    ])

    # input pathname -> partition
    src_re = r'/out/ldsc/regions/merged/([^/]+)/'

    # udf functions (NOTE: tissue needs to remove underscores used!)
    dataset = udf(lambda s: re.search(src_re, s).group(1).split('___')[0])
    annotation = udf(lambda s: re.search(src_re, s).group(1).split('___')[1].replace('_', ' '))
    tissue = udf(lambda s: re.search(src_re, s).group(1).split('___')[2].replace('_', ' '))
    biosample = udf(lambda s: re.search(src_re, s).group(1).split('___')[3].replace('_', ' '))
    method = udf(lambda s: re.search(src_re, s).group(1).split('___')[4].replace('_', ' '))
    assay_source = udf(lambda s: re.search(src_re, s).group(1).split('___')[5].replace('_', ' '))
    # load all regions, tissues, dataset, biosample, method, assay source and join
    df = spark.read.csv(srcdir, sep='\t', header=False, schema=schema) \
        .withColumn('source', input_file_name()) \
        .withColumn('annotation', annotation('source')) \
        .withColumn('tissue', tissue('source')) \
        .withColumn('dataset', dataset('source')) \
        .withColumn('biosample', biosample('source')) \
        .withColumn('method', method('source')) \
        .withColumn('assay_source', assay_source('source')) \
        .drop('source')

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

    # done
    spark.stop()


if __name__ == '__main__':
    main()
