from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = 's3://pkudtarkar-test/genetic_variant_effects/*'
    outdir = 's3://pkudtarkar-test/regions/variant_links'

    # load all variant prediciton regions
    df = spark.read.json(f'{srcdir}/part-*')

    # sort and write
    df.orderBy(['tissue', 'chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
