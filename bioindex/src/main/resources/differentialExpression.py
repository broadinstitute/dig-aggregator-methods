import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    diff_exp_dir = f'{s3_in}/differential_expression/*/diff_exp.json'
    summary_stats_dir = f'{s3_in}/differential_expression/*/summary_stats.json'
    outdir = f'{s3_bioindex}/diff_exp'

    diff_exp_df = spark.read.json(diff_exp_dir)

    diff_exp_df.orderBy(['gene', 'tissue', col('expression').desc()]) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/diff_exp')

    summary_stats_df = spark.read.json(summary_stats_dir)

    summary_stats_df.orderBy(['gene', 'p_value']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/summary_stats/gene')

    summary_stats_df.orderBy(['tissue', 'p_value']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/summary_stats/tissue')

    # done
    spark.stop()


if __name__ == '__main__':
    main()
