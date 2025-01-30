from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = 's3://dig-vision-genomics/annotated_regions/cis-regulatory_elements/*'
    bulk_outdir = 's3://dig-vision-genomics/bioindex/regions/bulk_atac/'
    sn_outdir = 's3://dig-vision-genomics/bioindex/regions/sn_atac/'

    # load all gene prediciton regions
    df = spark.read.json(f'{srcdir}/part-*')
    df = df.withColumn('biosample', regexp_replace(df.biosample, ',', ';'))
    bulk_df = df[df.source == 'ATAC-seq peaks']
    sn_df = df[df.source == 'snATAC-seq peaks']

    # sort and write
    bulk_df.orderBy(['chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(bulk_outdir)

    sn_df.orderBy(['species', 'chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(sn_outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
