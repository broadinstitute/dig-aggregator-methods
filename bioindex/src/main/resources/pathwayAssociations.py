import os
from pyspark.sql import SparkSession

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def process_magma(spark):
    """
    Load the MAGMA results and write them out by phenotype
    """
    df = spark.read.json(f'{s3_in}/out/magma/pathway-associations/*/*/')

    # partition dataframe
    mixed_df = df[df['ancestry'] == 'Mixed']
    non_mixed_df = df[df['ancestry'] != 'Mixed']

    # sort by gene, then by p-value
    mixed_df.orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f'{s3_bioindex}/pathway_associations/trans-ethnic')

    # sort by gene, then by p-value
    non_mixed_df.orderBy(['phenotype', 'ancestry', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f'{s3_bioindex}/pathway_associations/ancestry')


def main():
    # initialize spark
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    process_magma(spark)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
