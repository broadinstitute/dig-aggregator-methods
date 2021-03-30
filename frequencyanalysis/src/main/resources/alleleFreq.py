from pyspark.sql import SparkSession, Row, Window
from pyspark.sql.functions import rank


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('frequencyanalysis').getOrCreate()

    srcdir = 's3://dig-analysis-data/variants/*/*/*'
    outdir = 's3://dig-analysis-data/out/frequencyanalysis'

    # load all datasets
    df = spark.read.json(f'{srcdir}/part-*')
    df = df.select(df.varId, df.ancestry, df.eaf, df.maf) \
        .filter(df.ancestry != 'Mixed') \
        .filter(df.maf.isNotNull())

    # use window functions to get distinct variants on the same partitions
    df = df.rdd \
        .keyBy(lambda r: (r.varId, r.ancestry)) \
        .reduceByKey(lambda a, b: a if a.maf > b.maf else b) \
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
