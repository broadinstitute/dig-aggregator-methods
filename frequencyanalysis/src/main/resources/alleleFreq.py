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
        .filter(df.maf.isNotNull())

    # use window functions to get distinct variants on the same partitions
    window = Window.partitionBy(['varId', 'ancestry']) \
        .orderBy([df.varId, df.maf.desc()])

    # add the rank column and then only keep the max
    df = df.withColumn('rank', rank().over(window))
    df = df.filter(df.rank == 1)
    df = df.drop('rank')

    # write it out
    df.write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
