import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# what bucket will be output to?
OUT_BUCKET = f'dig-bio-{"test" if os.getenv("JOB_DRYRUN") else "index"}'


def main():
    """
    Arguments: none
    """
    srcdir = f's3://dig-analysis-data/genes/GRCh37/part-*'
    outdir = f's3://{OUT_BUCKET}/genes'

    # all valid chromosomes
    chromosomes = list(map(str, range(1, 23))) + ['X', 'Y', 'XY', 'M', 'MT']

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
