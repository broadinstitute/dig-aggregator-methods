import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# what bucket will be output to?
OUT_BUCKET = f'dig-bio-{"test" if os.getenv("JOB_DRYRUN") else "index"}'


def main():
    """
    Arguments: none
    """
    srcdir = f's3://dig-analysis-data/out/varianteffect/effects/*.json'
    outdir = f's3://{OUT_BUCKET}/variants/cqs'

    spark = SparkSession.builder.appName('vep').getOrCreate()

    # load the common effect data
    df = spark.read.json(srcdir)

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
    df.write.mode('overwrite').json(outdir)

    # done
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
