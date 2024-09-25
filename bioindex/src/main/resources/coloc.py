from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = 's3://dig-vision-genomics/coloc/*'
    outdir = 's3://dig-vision-genomics/bioindex/coloc/'

    # load all gene prediciton regions
    df = spark.read.json(f'{srcdir}/*.json.zst')

    # sort and write
    df.orderBy(['chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
