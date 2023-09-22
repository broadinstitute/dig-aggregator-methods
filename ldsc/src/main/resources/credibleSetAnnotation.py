#!/usr/bin/python3
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lit, col, sum

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


def get_region_df(spark, annotation, tissue):
    region_base = f's3a://{s3_in}/out/ldsc/regions/merged/annotation-tissue-biosample'
    region_files = f'{annotation}___{tissue}___*/*.csv'

    return spark.read.csv(f'{region_base}/{region_files}', sep='\t', header=False, schema=schema) \
        .withColumn('annotation', lit(annotation)) \
        .withColumn('tissue', lit(tissue)) \
        .drop('method', 'state')


def get_credible_set_df(spark, credible_set_path):
    credible_set_base = f's3a://{s3_in}/credible_sets/{credible_set_path}'
    return spark.read.json(f'{credible_set_base}/part-00000.json') \
        .select([
            col('chromosome').alias('cs_chromosome'),
            'position',
            'varId',
            'posteriorProbability',
            col('credibleSetId').alias('riskSignal')
        ])


def get_grouped_df(region_df, credible_set_df):
    # join credible sets into all regions matching the sql-like join
    variant_in_region = \
        (region_df.chromosome == credible_set_df.cs_chromosome) & \
        (region_df.start <= credible_set_df.position) & \
        (region_df.end > credible_set_df.position)
    df = region_df.join(credible_set_df, variant_in_region) \
        .drop('cs_chromosome', 'start', 'end')

    # aggregate posterior probability
    return df \
        .groupBy(['riskSignal', 'annotation', 'tissue', 'biosample'])\
        .agg(sum('posteriorProbability').alias('posteriorProbability'))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--annotation', type=str, required=True,
                        help="Annotation from merged regions")
    parser.add_argument('--tissue', type=str, required=True,
                        help="Tissue from merged regions")
    parser.add_argument('--credible-set-path', type=str, required=True,
                        help="Credible set path in form of <dataset>/<phenotype>")
    args = parser.parse_args()

    # create a spark session
    spark = SparkSession.builder.appName('ldsc').getOrCreate()

    region_df = get_region_df(spark, args.annotation, args.tissue)
    credible_set_df = get_credible_set_df(spark, args.credible_set_path)

    df = get_grouped_df(region_df, credible_set_df)

    df.write \
        .mode('overwrite') \
        .json(f's3://{s3_out}/out/ldsc/regions/credible_sets/{args.credible_set_path}/{args.annotation}/{args.tissue}/')

    spark.stop()

# entry point
if __name__ == '__main__':
    main()
