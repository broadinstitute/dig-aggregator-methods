from pyspark.sql import SparkSession
from pyspark.sql.functions import udf


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = 's3://dig-analysis-data/annotated_regions/genetic_variant_effects/*'
    outdir = 's3://dig-bio-index/regions/variant_links'

    tissue = udf(lambda s: s.replace('_', ' '))

    df = spark.read.json(f'{srcdir}/part-*') \
        .withColumn('tissue', tissue('tissue'))

    # sort and write
    df.orderBy(['chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
