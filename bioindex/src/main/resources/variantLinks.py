from pyspark.sql import SparkSession


s3_dir = 'dig-vision-genomics'
bioindex_dir = 'dig-vision-genomics/bioindex'

def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = f's3://{s3_dir}/annotated_regions/genetic_variant_effects/*'
    outdir = f's3://{bioindex_dir}/regions/variant_links'

    # load all variant prediciton regions
    df = spark.read.json(f'{srcdir}/*.json.zst')

    # sort and write
    df.orderBy(['tissue', 'chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(outdir + '/tissue')

    # sort and write
    df.orderBy(['gene']) \
        .write \
        .mode('overwrite') \
        .json(outdir + '/gene')

    # sort and write
    df.orderBy(['chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(outdir + '/region')

    # done
    spark.stop()


if __name__ == '__main__':
    main()
