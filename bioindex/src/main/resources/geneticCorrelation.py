import numpy as np
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import when

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def process_datasets(spark):
    srcdir = f'{s3_in}/out/ldsc/genetic_correlation/*/*.json'
    outdir = f'{s3_bioindex}/genetic-correlation'

    df = spark.read.json(srcdir)

    # filter out all entries with pValue >= 0.05
    df = df[df['project'] == 'cancer']

    # Set min pValue to smalled numpy 64-bit value
    df = df.withColumn('pValue', when(df.pValue == 0.0, np.nextafter(0, 1)).otherwise(df.pValue))

    # partition dataframe
    mixed_df = df[df['ancestry'] == 'Mixed']
    non_mixed_df = df[df['ancestry'] != 'Mixed']

    # For mixed: sort by phenotype, then by p-value
    mixed_df.orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/trans-ethnic')

    # For non-mixed: sort by phenotype, ancestry, then by p-value
    non_mixed_df.orderBy(['phenotype', 'ancestry', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/ancestry')


def main():
    # initialize spark
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    process_datasets(spark)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
