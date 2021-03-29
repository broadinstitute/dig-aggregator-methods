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
    df = df.select(df.varId, df.maf) \
        .filter(df.maf.isNotNul())

    # find the maximum maf per variant
    df = df.orderBy([df.varId, df.maf.desc()])
    df = df.dropDuplicates([df.varId])

    # write it out
    df.write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
