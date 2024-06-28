import boto3
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def check_path(path):
    bucket, non_bucket_path = re.findall('s3://([^/]*)/(.*)', path)[0]
    s3 = boto3.client('s3')
    return s3.list_objects_v2(Bucket=bucket, Prefix=non_bucket_path)['KeyCount'] > 0


def main():
    """
    Arguments: none
    """
    common_dir = f'{s3_in}/out/huge/common/*/part-*'
    rare_dir = f'{s3_in}/out/huge/rare'
    gene_outdir = f'{s3_bioindex}/huge/gene'
    phenotype_outdir = f'{s3_bioindex}/huge/phenotype'

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load the common data
    df = spark.read.json(common_dir) \
        .select('gene', 'phenotype', 'chromosome', 'start', 'end', 'bf_common')

    if check_path(rare_dir):
        rare_df = spark.read.json(f'{rare_dir}/*/part-*') \
            .select('gene', 'phenotype', 'bf_rare')

        df = df.join(rare_df, on=['gene', 'phenotype'], how='left')
        df = df.fillna({'bf_rare': 1.0})
    else:
        df = df.withColumn('bf_rare', lit(1.0))

    # Add in huge value
    df = df.withColumn('huge', df.bf_common * df.bf_rare)

    # index by gene
    df.orderBy(col("gene").asc(), col("huge").desc()) \
        .write \
        .mode('overwrite') \
        .json(gene_outdir)

    # index by
    df.orderBy(col("phenotype").asc(), col("huge").desc()) \
        .write \
        .mode('overwrite') \
        .json(phenotype_outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
