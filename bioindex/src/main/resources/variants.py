import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, concat_ws, explode, regexp_replace, row_number, split
from pyspark.sql.types import IntegerType

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = f'{s3_in}/out/varianteffect/common/part-*'
    outdir = f'{s3_bioindex}/variants/common/'

    df = spark.read.json(srcdir)

    # add chromosome and position
    df = df.withColumn('chromosome',  split(df.varId, ':').getItem(0)) \
        .withColumn('position', split(df.varId, ':').getItem(1).cast(IntegerType()))

    # sort by chromosome, position
    df.orderBy(['chromosome', 'position']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    spark.stop()

# entry point
if __name__ == '__main__':
    main()
