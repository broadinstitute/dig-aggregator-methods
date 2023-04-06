import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from datetime import datetime


def main():
    """
    Arguments: none
    """
    arg_parser = argparse.ArgumentParser(prog='huge-cache.py')
    arg_parser.add_argument("--cqs", help="Variant CQS data", required=True)
    arg_parser.add_argument("--nearest-genes", help="Variant effect data", required=True)
    arg_parser.add_argument("--cache-dir", help="The cache directory to write to", required=True)
    cli_args = arg_parser.parse_args()
    files_glob = 'part-*'
    variant_cqs_glob = cli_args.cqs + files_glob
    nearest_genes_glob = cli_args.nearest_genes + files_glob
    cache_dir = cli_args.cache_dir
    spark = SparkSession.builder.appName('huge-cache').getOrCreate()
    cqs_cache = spark.read.json(variant_cqs_glob).select('varId', 'impact', 'geneId').filter(col("pick") == 1)
    nearest_genes_cache = spark.read.json(nearest_genes_glob).select("varId", "nearest_gene")
    cache = cqs_cache.join(nearest_genes_cache, ["varId"], "outer")
    cache.write.mode('overwrite').json(cache_dir)
    spark.stop()


if __name__ == '__main__':
    main()
