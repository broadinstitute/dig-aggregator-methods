import os

from pyspark.sql import SparkSession

# what bucket will be output to?
OUT_BUCKET = f'dig-bio-{"test" if os.getenv("JOB_DRYRUN") else "index"}'


def main():
    """
    Arguments: none
    """
    srcdir = 's3://dig-analysis-data/transcription_factors/part-*'
    outdir = f's3://{OUT_BUCKET}/variants/transcription_factors'

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load all the unique variants
    df = spark.read.json(srcdir)

    # write out variant data by ID
    df.orderBy(['varId']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
