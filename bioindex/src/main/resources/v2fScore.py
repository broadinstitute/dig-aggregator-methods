import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = f'{s3_in}/annotated_regions/V2Fscores/*'
    outdir = f'{s3_bioindex}/regions/v2f_scores'

    df = spark.read.json(f'{srcdir}/*.json.zstd')

    # sort and write
    df.orderBy(['chromosome', 'start', col('probScore').desc()]) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
