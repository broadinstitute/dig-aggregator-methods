import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, IntegerType

# what bucket will be output to?
OUT_BUCKET = f'dig-bio-{"test" if os.getenv("JOB_DRYRUN") else "index"}'


def main():
    """
    Arguments: none
    """
    srcdir = f's3://dig-analysis-data/out/metaanalysis/top/*/part-*'
    outdir = f's3://{OUT_BUCKET}/associations'

    # common vep data
    common_dir = 's3://dig-analysis-data/out/varianteffect/common'

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load the top associations for each phenotype
    df = spark.read.json(srcdir)
    common = spark.read.json(common_dir)

    # join the common data
    df = df.join(common, 'varId', how='left_outer')

    # sort by phenotype and then position
    df.orderBy(['phenotype', 'chromosome', 'position']) \
        .write \
        .mode('overwrite') \
        .json('%s/top' % outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
