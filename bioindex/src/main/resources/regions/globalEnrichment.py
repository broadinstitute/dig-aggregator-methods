import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, input_file_name, lit, struct, udf, when

# what bucket will be output to?
OUT_BUCKET = f'dig-bio-index'  #{"test" if os.getenv("JOB_DRYRUN") else "index"}'


def main():
    """
    Arguments: none
    """
    srcdir = f's3://dig-analysis-data/out/gregor/enrichment/*/part-*'
    outdir = f's3://{OUT_BUCKET}/global_enrichment'

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load the trans-ethnic, meta-analysis, top variants and write them sorted
    df = spark.read.json(srcdir)

    # sorted by phenotype
    df.orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/phenotype')

    # done
    spark.stop()


if __name__ == '__main__':
    main()
