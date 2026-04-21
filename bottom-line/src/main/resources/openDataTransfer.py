import argparse
import glob
import os
from scipy.stats import norm
import shutil
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType


s3_in = os.environ['INPUT_PATH']
s3_open_data = 's3://dig-open-bottom-line-analysis-stg'

@udf(returnType=DoubleType())
def p_to_z(p_value: float) -> float:
    return float(abs(norm.ppf(p_value / 2.0)))

def check_existence(path):
    return subprocess.call(['aws', 's3', 'ls', path, '--recursive'])

def upload(phenotype, ancestry, df):
    tmp_path = f'{s3_open_data}/tmp/{ancestry}_{phenotype}_sumstats'
    output_path = f'{s3_open_data}/bottom-line/{ancestry}/{phenotype}.sumstats.tsv.gz'
    df.coalesce(1).write \
        .option('header', 'true') \
        .option('delimiter', '\t') \
        .option('compression', 'gzip') \
        .mode('overwrite') \
        .format('csv') \
        .save(tmp_path)
    subprocess.check_call(['aws', 's3', 'mv', f'{tmp_path}/', 'tmp/', '--recursive'])
    part_files = glob.glob('tmp/part-*')
    if len(part_files) != 1:
        raise Exception(f'Incorrect number of files written ({len(part_files)})')
    subprocess.check_call(['aws', 's3', 'mv', f'{part_files[0]}', output_path])
    shutil.rmtree('tmp')

def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--phenotype', type=str, required=True)
    opts.add_argument('--ancestry', type=str, required=True)
    args = opts.parse_args()

    path = f'trans-ethnic/{args.phenotype}' if args.ancestry == 'Mixed' else \
        f'ancestry-specific/{args.phenotype}/ancestry={args.ancestry}'

    # initialize spark session
    spark = SparkSession.builder.appName('Open Data Transfer').getOrCreate()
    srcdir = f'{s3_in}/out/metaanalysis/bottom-line/{path}'
    minp_dir = f'{s3_in}/out/metaanalysis/min_p/{path}'
    largest_dir = f'{s3_in}/out/metaanalysis/largest/{path}'
    
    df = spark.read \
        .json(srcdir)
    # replace stdErr (from naive metaanalysis) with back calculated value (from overlap aware metaanalysis)
    df = df.withColumn('stdErr', df.beta / p_to_z(df.pValue))

    min_p = spark.read \
        .json(minp_dir) \
        .select(col('varId'),
                col('pValue').alias('min_p_pValue'),
                col('beta').alias('min_p_beta'),
                col('n').alias('min_p_n'))

    if not check_existence(f'{largest_dir}/'):
        largest = spark.read \
            .json(largest_dir) \
            .select(col('varId'),
                    col('pValue').alias('largest_pValue'),
                    col('beta').alias('largest_beta'),
                    col('n').alias('largest_n'))

        df = df.join(min_p, on='varId', how='left').join(largest, on='varId', how='left')
    else:
        print(f'Warning: {largest_dir} does not exist, skipping largest join')
        df = df.join(min_p, on='varId', how='left')

    upload(args.phenotype, args.ancestry, df)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
