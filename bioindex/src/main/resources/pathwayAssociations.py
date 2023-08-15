from pyspark.sql import SparkSession

OUTDIR = 's3://dig-bio-index/'

s3_in = 'dig-analysis-pxs'
s3_out = 'dig-analysis-pxs/bioindex'


def process_magma(spark):
    """
    Load the MAGMA results and write them out by phenotype
    """
    df = spark.read.json('s3://dig-analysis-data/out/magma/pathway-associations/*/*/')

    # partition dataframe
    mixed_df = df[df['ancestry'] == 'Mixed']
    non_mixed_df = df[df['ancestry'] != 'Mixed']

    # sort by gene, then by p-value
    mixed_df.orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f's3://{s3_out}/pathway_associations/trans-ethnic')

    # sort by gene, then by p-value
    non_mixed_df.orderBy(['phenotype', 'ancestry', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f's3://{s3_out}/pathway_associations/ancestry-specific')


def main():
    # initialize spark
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    process_magma(spark)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
