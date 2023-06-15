import re

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import input_file_name, udf


def main():
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output directories
    srcdir = f's3://dig-analysis-data/out/ldsc/regions/merged/annotation-tissue/*/*.csv'
    outdir = 's3://dig-bio-index/regions'

    # input summary stats schema
    schema = StructType([
        StructField('chromosome', StringType(), nullable=False),
        StructField('start', IntegerType(), nullable=False),
        StructField('end', IntegerType(), nullable=False),
        StructField('state', StringType(), nullable=True),
        StructField('biosample', StringType(), nullable=True),
        StructField('method', StringType(), nullable=True),
        StructField('source', StringType(), nullable=True),
        StructField('dataset', StringType(), nullable=True)
    ])

    # input pathname -> partition
    src_re = r'/out/ldsc/regions/merged/annotation-tissue/([^/]+)/'

    # udf functions (NOTE: tissue needs to remove underscores used!)
    annotation = udf(lambda s: re.search(src_re, s).group(1).split('___')[0])
    tissue = udf(lambda s: re.search(src_re, s).group(1).split('___')[1].replace('_', ' '))

    df = spark.read.csv(srcdir, sep='\t', header=False, schema=schema) \
        .withColumn('file_name', input_file_name()) \
        .withColumn('annotation', annotation('file_name')) \
        .withColumn('tissue', tissue('file_name'))
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

    # done
    spark.stop()


if __name__ == '__main__':
    main()
