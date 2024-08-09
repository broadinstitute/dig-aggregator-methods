import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']

replace = udf(lambda s: s.replace('_', ' '))


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = f'{s3_in}/annotated_regions/target_gene_links/*'
    outdir = f'{s3_bioindex}/regions/gene_links'

    df = spark.read.json(f'{srcdir}/part-*')
    df = df.withColumn('tissue', replace('tissue'))

    # sort and write
    df.orderBy(['tissue', 'chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
