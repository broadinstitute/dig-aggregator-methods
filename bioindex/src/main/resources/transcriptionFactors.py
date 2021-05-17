from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.types import IntegerType


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output directories
    srcdir = 's3://dig-analysis-data/transcription_factors/*/part-*'
    outdir = 's3://dig-bio-index/transcription_factors'

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
