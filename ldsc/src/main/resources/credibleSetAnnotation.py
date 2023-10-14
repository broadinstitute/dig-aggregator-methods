#!/usr/bin/python3
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lit, col, sum, input_file_name, udf
import re

s3_in = 'dig-analysis-data'
s3_out = 'dig-analysis-data'

schema = StructType([
    StructField('chromosome', StringType(), nullable=False),
    StructField('start', IntegerType(), nullable=False),
    StructField('end', IntegerType(), nullable=False),
    StructField('state', StringType(), nullable=True),
    StructField('biosample', StringType(), nullable=True),
    StructField('method', StringType(), nullable=True),
    StructField('source', StringType(), nullable=True),
    StructField('dataset', StringType(), nullable=True)
])

src_re = r'/out/ldsc/regions/merged/annotation-tissue-biosample/([^/]+)/'
annotation = udf(lambda s: re.search(src_re, s).group(1).split('___')[0])
tissue = udf(lambda s: re.search(src_re, s).group(1).split('___')[1].replace('_', ' '))


def get_region_df(spark):
    region_url = f's3://{s3_in}/out/ldsc/regions/merged/annotation-tissue-biosample/*___*___*/*.csv'

    df = spark.read.csv(f'{region_url}', sep='\t', header=False, schema=schema) \
        .withColumn('file_name', input_file_name()) \
        .withColumn('annotation', annotation('file_name')) \
        .withColumn('tissue', tissue('file_name'))
    return df.drop('file_name', 'method', 'state')


def get_credible_set_df(spark, credible_set_path):
    credible_set_base = f's3://{s3_in}/credible_sets/{credible_set_path}'
    phenotype = credible_set_path.split('/')[1]
    df = spark.read.json(f'{credible_set_base}/part-00000.json') \
        .withColumn('phenotype', lit(phenotype)) \
        .select([
            col('chromosome').alias('cs_chromosome'),
            'position',
            'varId',
            'posteriorProbability',
            'credibleSetId',
            'phenotype'
        ])
    return df.filter(df.posteriorProbability.isNotNull())


def get_grouped_df(region_df, credible_set_df):
    # join credible sets into all regions matching the sql-like join
    variant_in_region = \
        (region_df.chromosome == credible_set_df.cs_chromosome) & \
        (region_df.start <= credible_set_df.position) & \
        (region_df.end > credible_set_df.position)
    df = credible_set_df.join(region_df, variant_in_region) \
        .drop('cs_chromosome', 'start', 'end')

    # aggregate posterior probability
    return df \
        .groupBy(['credibleSetId', 'phenotype', 'annotation', 'tissue', 'biosample'])\
        .agg(sum('posteriorProbability').alias('posteriorProbability'))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--credible-set-path', type=str, required=True,
                        help="Credible set path in form of <dataset>/<phenotype>")
    args = parser.parse_args()

    # create a spark session
    spark = SparkSession.builder.appName('ldsc').getOrCreate()

    region_df = get_region_df(spark)
    credible_set_df = get_credible_set_df(spark, args.credible_set_path)

    df = get_grouped_df(region_df, credible_set_df)

    # partition by annotation, tissue (but can't drop those fields)
    df.write \
        .mode('overwrite') \
        .json(f's3://{s3_out}/out/ldsc/regions/credible_sets/{args.credible_set_path}/')

    spark.stop()

# entry point
if __name__ == '__main__':
    main()
