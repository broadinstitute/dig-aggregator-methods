import os
from pyspark.sql import SparkSession

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output directories
    srcdir = f'{s3_in}/out/varianteffect/variants/cqs/*.json'
    outdir = f'{s3_bioindex}/transcript_consequences'

    # load the common effect data
    df = spark.read.json(srcdir)

    # output the consequences, ordered by variant so they are together
    df.orderBy(['chromosome', 'position']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
