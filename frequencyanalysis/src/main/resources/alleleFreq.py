from pyspark.sql import SparkSession, Row


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('frequencyanalysis').getOrCreate()

    srcdir = 's3://dig-analysis-data/variants/*/*/*'
    outdir = 's3://dig-analysis-data/out/frequencyanalysis'

    # load all datasets
    df = spark.read.json(f'{srcdir}/part-*')

    # find the maximum maf per variant
    df = df.filter(df.maf.isNotNull()) \
        .rdd \
        .keyBy(lambda r: r.varId) \
        .reduceByKey(max) \
        .map(lambda r: r[1]) \
        .toDF()

    # write it out
    df.write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
