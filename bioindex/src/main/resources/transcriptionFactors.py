from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output directories
    srcdir = 's3://dig-analysis-data/transcription_factors/*/part-*'
    outdir = 's3://dig-bio-index/transcription_factors'

    # common vep data
    common_dir = 's3://dig-analysis-data/out/varianteffect/common'

    # load all the unique variants
    df = spark.read.json(srcdir)
    common = spark.read.json(common_dir).select('varId', 'dbSNP')

    # keep only certain columns
    df = df.select(
        df.varId,
        df.positionWeightMatrix,
        df.delta,
        df.strand,
        df.refScore,
        df.altScore,
    )

    # join to get dbSNP, sort, and write
    df.join(common, 'varId', how='inner') \
        .orderBy(['varId']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
