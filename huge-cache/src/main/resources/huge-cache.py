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
    print('Hello! The time is now ', now_str())
    print('Now building argument parser')
    arg_parser = argparse.ArgumentParser(prog='huge-cache.py')
    arg_parser.add_argument("--cqs", help="Variant CQS data", required=True)
    arg_parser.add_argument("--effects", help="Variant effect data", required=True)
    arg_parser.add_argument("--gene-ids-map", help="Map from gene symbol to Ensembl id", required=True)
    arg_parser.add_argument("--cache-dir", help="The cache directory to write to", required=True)
    print('Now parsing CLI arguments')
    cli_args = arg_parser.parse_args()
    files_glob = 'part-*'
    variant_cqs_glob = cli_args.cqs + files_glob
    variant_effects_glob = cli_args.effects + files_glob
    gene_ids_map_glob = cli_args.gene_ids_map + files_glob
    cache_dir = cli_args.cache_dir
    print('Variant CQS data: ', variant_cqs_glob)
    print('Variant effects data: ', variant_effects_glob)
    print('Gene ids map data: ', gene_ids_map_glob)
    spark = SparkSession.builder.appName('huge-cache').getOrCreate()
    cqs_cache = spark.read.json(variant_cqs_glob).select('varId', 'impact', 'geneId')
    inspect_df(cqs_cache, "CQS cache")
    print('Now reading variant effects.')
    effects_cache_symbol = \
        spark.read.json(variant_effects_glob).select('id', 'nearest')\
            .withColumnRenamed('id', 'varId').withColumnRenamed('nearest', 'nearest_list')\
            .withColumn('nearest_symbol', col('nearest_list').getItem(0)).drop('nearest_list')
    inspect_df(effects_cache_symbol, "variant effects cache")
    gene_ids_map = spark.read.json(gene_ids_map_glob).select("symbol", "ensembl")\
        .withColumnRenamed("symbol", "nearest_symbol").withColumnRenamed("ensembl", "nearest_ensembl")
    inspect_df(gene_ids_map, "gene ids map")
    effects_cache = effects_cache_symbol.join(gene_ids_map, ["nearest_symbol"])
    inspect_df(effects_cache, "variant effects cache")
    cache = cqs_cache.join(effects_cache, ['varId'], 'outer')
    inspect_df(cache, "cache")
    print('Now writing cache to', cache_dir)
    cache.write.mode('overwrite').json(cache_dir)
    print('Done writing variants effects cache')
    print('Done with work, therefore stopping Spark')
    spark.stop()
    print('Spark stopped')


if __name__ == '__main__':
    main()
