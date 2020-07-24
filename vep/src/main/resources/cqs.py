#!/usr/bin/python3

import os.path
import platform

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# where in S3 VEP data (input and output) is
S3DIR = 's3://dig-analysis-data/out/varianteffect'


def main():
    """
    Arguments: none
    """
    print('Python version: %s' % platform.python_version())
    print('user=%s' % os.getenv('USER'))

    # create the spark context
    spark = SparkSession.builder.appName('vep').getOrCreate()

    # load the common effect data
    df = spark.read.json('%s/effects/*.json' % S3DIR)

    # extract just the consequences
    df = df.select(df.id, df.transcript_consequences) \
        .withColumn('cqs', explode(df.transcript_consequences)) \
        .select(
            col('id').alias('varId'),
            col('cqs.*'),
        )

    # clean up the frame
    df = df.drop('domains')

    # output the consequences
    df.write.mode('overwrite').json('%s/cqs' % S3DIR)

    # done
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
