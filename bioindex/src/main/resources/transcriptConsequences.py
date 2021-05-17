from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output directories
    srcdir = f's3://dig-analysis-data/out/varianteffect/cqs/*.json'
    outdir = f's3://dig-bio-index/transcript_consequences'

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
