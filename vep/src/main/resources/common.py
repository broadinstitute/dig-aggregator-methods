#!/usr/bin/python3

import os.path
import platform

from pyspark.sql import SparkSession

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

    # load data
    df = spark.read.json('%s/effects/*.json' % S3DIR)
    snp = spark.read.csv('%s/snp/*.csv' % S3DIR, sep='\t', header=True)

    # use the most severe consequence to get genes and impact
    df = df.select(
        df.id.alias('varId'),
        df.most_severe_consequence.alias('consequence'),
        df.nearest,
    )

    # join with the dbSNP
    df = df.join(snp, 'varId', how='left_outer')

    # output the common data in json format
    df.write.mode('overwrite').json('%s/common' % S3DIR)

    # done
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
