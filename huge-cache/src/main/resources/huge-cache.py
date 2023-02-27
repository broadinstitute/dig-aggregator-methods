import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from datetime import datetime


def now_str():
    return str(datetime.now())


def inspect_df(df: DataFrame, name: str):
    n_rows = df.count()
    print("Dataframe ", name, " has ", n_rows, " rows.", "(", now_str(), ")")
    df.printSchema()
    n_rows_max = 23
    if n_rows > n_rows_max:
        df.sample(fraction=(n_rows_max / n_rows)).show()
    else:
        df.show()
    print('Done showing ', name, ' at ', now_str())


def main():
    """
    Arguments: none
    """
    # print('Hello! The time is now ', now_str())
    # print('Now building argument parser')
    arg_parser = argparse.ArgumentParser(prog='huge-cache.py')
    arg_parser.add_argument("--cqs", help="Variant CQS data", required=True)
    arg_parser.add_argument("--nearest-genes", help="Variant effect data", required=True)
    arg_parser.add_argument("--cache-dir", help="The cache directory to write to", required=True)
    # print('Now parsing CLI arguments')
    cli_args = arg_parser.parse_args()
    files_glob = 'part-*'
    variant_cqs_glob = cli_args.cqs + files_glob
    nearest_genes_glob = cli_args.nearest_genes + files_glob
    cache_dir = cli_args.cache_dir
    # print('Variant CQS data: ', variant_cqs_glob)
    # print('Nearest genes data: ', nearest_genes_glob)
    # print('Cache dir: ', cache_dir)
    spark = SparkSession.builder.appName('huge-cache').getOrCreate()
    cqs_cache = spark.read.json(variant_cqs_glob).select('varId', 'impact', 'geneId').filter(col("pick") == 1)
    # print('Now reading variant effects.')
    nearest_genes_cache = spark.read.json(nearest_genes_glob).select("varId", "nearest_gene")
    cache = cqs_cache.join(nearest_genes_cache, ["varId"], "outer")
    # print('Now writing cache to', cache_dir)
    cache.write.mode('overwrite').json(cache_dir)
    # print('Done writing variants effects cache')
    # print('Done with work, therefore stopping Spark')
    spark.stop()
    # print('Spark stopped')


if __name__ == '__main__':
    main()
