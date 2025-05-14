import os
from pyspark.sql import SparkSession
import argparse
import boto3
from urllib.parse import urlparse
import re

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype_ancestry')
    args = opts.parse_args()
    phenotype, ancestry = args.phenotype_ancestry.split('/')
    path = f"trans-ethnic/{phenotype}" if ancestry == 'Mixed' else f"ancestry-specific/{phenotype}/ancestry={ancestry}"


    # initialize spark session
    spark = SparkSession.builder.appName('Open Data Converter').getOrCreate()
    srcdir = f"{s3_in}/out/metaanalysis/bottom-line/{path}"
    df = spark.read.option("compression", "org.apache.hadoop.io.compress.ZStandardCodec").json(srcdir)
    tmp_output_path = f"s3://dig-open-bottom-line-analysis-stg/tmp/{ancestry}_{phenotype}_sumstats"
    df.coalesce(1).write \
        .option("header", "true") \
        .option("delimiter", "\t") \
        .option("compression", "gzip") \
        .mode("overwrite") \
        .format("csv") \
        .save(tmp_output_path)

    s3 = boto3.client("s3")
    parsed = urlparse(tmp_output_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")

    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    part_file = next(obj['Key'] for obj in resp['Contents'] if re.search(r'part-.*\.gz$', obj['Key']))

    # Copy it to the desired location
    target_key = f"bottom-line/{ancestry}/{phenotype}.sumstats.tsv.gz"
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": part_file},
        Key=target_key
    )

    # Optionally clean up the temp directory
    s3.delete_object(Bucket=bucket, Key=part_file)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
