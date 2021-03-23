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

    # load data
    df = spark.read.json('%s/effects/*.json' % S3DIR)
    snp = spark.read.csv('%s/snp/*.csv' % S3DIR, sep='\t', header=True)

    # explode the consequences to get allele frequency
    maf = df.select(df.id.alias('varId'), df.transcript_consequences) \
        .withColumn('cqs', explode(df.transcript_consequences)) \
        .select(
            col('varId'),
            col('cqs.gnomad_genomes_popmax_af').alias('maf'),
        )

    # keep the max frequency per variant
    maf = maf.groupBy('varId').max('maf') \
        .withColumnRenamed('max(maf)', 'maf')

    # keep non-consequence field
    df = df.select(
        df.id.alias('varId'),
        df.most_severe_consequence.alias('consequence'),
        df.nearest,
    )

    # join with frequency and dbSNP
    df = df \
        .join(maf, 'varId', how='left_outer') \
        .join(snp, 'varId', how='left_outer')

    # output the common data in json format
    df.write.mode('overwrite').json('%s/common' % S3DIR)

    # done
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
