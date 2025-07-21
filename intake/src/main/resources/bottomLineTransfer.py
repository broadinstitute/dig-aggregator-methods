import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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
    min_p_path = f"out/metaanalysis/min_p/{path}"
    largest_path = f"out/metaanalysis/largest/{path}"
    
    min_p_dir = f"{s3_in}/{min_p_path}"
    largest_dir = f"{s3_in}/{largest_path}"
    
    df = spark.read.option("compression", "org.apache.hadoop.io.compress.ZStandardCodec").json(srcdir)
    min_p = (spark.read.option("compression", "org.apache.hadoop.io.compress.ZStandardCodec")
             .json(min_p_dir).select(col("varId"),
                                     col("pValue").alias("min_p_pValue"),
                                     col("beta").alias("min_p_beta"),
                                     col("n").alias("min_p_n")))

    largest = (spark.read.option("compression", "org.apache.hadoop.io.compress.ZStandardCodec")
               .json(largest_dir).select(col("varId"),
                                         col("pValue").alias("largest_pValue"),
                                         col("beta").alias("largest_beta"),
                                         col("n").alias("largest_n")))
    
    # Join the dataframes
    df = df.join(min_p, on="varId", how="left").join(largest, on="varId", how="left")
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
    
    # Use multipart copy for large files
    copy_source = {"Bucket": bucket, "Key": part_file}
    s3.copy(copy_source, bucket, target_key)

    # Optionally clean up the temp directory
    s3.delete_object(Bucket=bucket, Key=part_file)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
