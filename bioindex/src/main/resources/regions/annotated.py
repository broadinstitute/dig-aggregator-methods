import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit, regexp_replace, struct, when

# what bucket will be output to?
OUT_BUCKET = f'dig-bio-index'  #{"test" if os.getenv("JOB_DRYRUN") else "index"}'


def main():
    """
    Arguments: none
    """
    srcdir = f's3://dig-analysis-data/out/gregor/regions/joined/part-*'
    outdir = f's3://{OUT_BUCKET}/regions'

    #initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load all regions, fix the tissue ID, sort, and write
    df = spark.read.json(srcdir)

    # tissue id and locus
    # df.filter() \
    #     .orderBy(['tissueId', 'chromosome', 'start']) \
    #     .write \
    #     .mode('overwrite') \
    #     .json(f'{outdir}/tissue')

    # sort by annotation and locus
    df.orderBy(['annotation', 'chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/annotation')

    # done
    spark.stop()


if __name__ == '__main__':
    main()
