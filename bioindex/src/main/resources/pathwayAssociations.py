from pyspark.sql import SparkSession

OUTDIR = 's3://dig-bio-index/pathway_associations'


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
        .json(f'{OUTDIR}/trans-ethnic')

    # sort by gene, then by p-value
    non_mixed_df.orderBy(['phenotype', 'ancestry', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f'{OUTDIR}/ancestry-specific')


def main():
    # initialize spark
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    process_magma(spark)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
