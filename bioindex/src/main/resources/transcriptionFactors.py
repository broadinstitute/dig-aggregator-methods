import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.types import IntegerType

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output directories
    srcdir = f'{s3_in}/transcription_factors/*/part-*'
    outdir = f'{s3_bioindex}/transcription_factors'

    # load all the unique variants
    df = spark.read.json(srcdir)

    # keep only certain columns; extract chromosome and position from varId
    df = df.select(
        df.varId,
        split(df.varId, ':').getItem(0).alias('chromosome'),
        split(df.varId, ':').getItem(1).cast(IntegerType()).alias('position'),
        df.positionWeightMatrix,
        df.delta,
        df.strand,
        df.refScore,
        df.altScore,
    )

    # join to get dbSNP, sort, and write
    df.orderBy(['chromosome', 'position']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
