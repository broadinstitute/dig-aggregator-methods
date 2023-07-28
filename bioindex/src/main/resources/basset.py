from pyspark.sql import SparkSession
from pyspark.sql.functions import desc


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = 's3://dig-analysis-data/out/basset/translated'
    outdir = 's3://dig-bio-index/basset'

    # load all variant prediciton regions
    df = spark.read.json(f'{srcdir}/part-*')

    # sort and write
    df.orderBy('chromosome', 'position', desc('p')) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/locus')

    # done
    spark.stop()


if __name__ == '__main__':
    main()
