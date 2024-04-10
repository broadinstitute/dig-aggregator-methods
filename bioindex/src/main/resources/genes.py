import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    """
    Arguments: none
    """
    srcdir = f'{s3_in}/genes/GRCh37/part-*'
    outdir = f'{s3_bioindex}/genes'

    # all valid chromosomes
    chromosomes = list(map(str, range(1, 23))) + ['X', 'Y', 'XY', 'MT']

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load the genes and write them sorted
    df = spark.read.json(srcdir) \
        .filter(col('chromosome').isin(*chromosomes))

    # index by position
    df.coalesce(1) \
        .orderBy(['chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
