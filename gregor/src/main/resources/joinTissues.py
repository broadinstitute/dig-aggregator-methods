from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('gregor').getOrCreate()

    # source and output directories
    outdir = f's3://dig-analysis-data/out/gregor/regions/joined'

    # load all regions, tissues, and join
    df = spark.read.json('s3://dig-analysis-data/annotated_regions/*/*/part-*')
    tissues_df = spark.read.json('s3://dig-analysis-data/tissues/ontology/part-*')

    # fix up the regions
    df = df \
        .withColumnRenamed('name', 'annotation') \
        .withColumnRenamed('biosample', 'tissueId')

    # join with the tissue ontology
    df = df.join(tissues_df, on='tissueId', how='left_outer')

    # write out with tissue descriptions
    df.write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
