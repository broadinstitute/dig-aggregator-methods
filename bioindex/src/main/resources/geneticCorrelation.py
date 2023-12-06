import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import when

s3_in = 'dig-analysis-hermes'
s3_out = 'dig-analysis-hermes/bioindex'
OUTDIR = f's3://{s3_out}/genetic-correlation'


def process_datasets(spark):
    df = spark.read.json(f's3://{s3_in}/out/ldsc/genetic_correlation/*/*.json')

    # Set min pValue to smalled numpy 64-bit value
    df = df.withColumn('pValue', when(df.pValue == 0.0, np.nextafter(0, 1)).otherwise(df.pValue))

    # partition dataframe
    mixed_df = df[df['ancestry'] == 'Mixed']
    non_mixed_df = df[df['ancestry'] != 'Mixed']

    # For mixed: sort by phenotype, then by p-value
    mixed_df.orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f's3://{s3_out}/genetic-correlation/trans-ethnic')

    # For non-mixed: sort by phenotype, ancestry, then by p-value
    non_mixed_df.orderBy(['phenotype', 'ancestry', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f's3://{s3_out}/genetic-correlation/ancestry-specific')


def main():
    # initialize spark
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    process_datasets(spark)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
