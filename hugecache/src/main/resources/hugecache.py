import argparse
from pyspark.sql import SparkSession, DataFrame
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
    arg_parser = argparse.ArgumentParser(prog='huge.py')
    arg_parser.add_argument("--cqs", help="Variant CQS data", required=True)
    arg_parser.add_argument("--effects", help="Variant effect data", required=True)
    arg_parser.add_argument("--cqs-cache-dir", help="Variant CQS data cache directory", required=True)
    arg_parser.add_argument("--effects-cache-dir", help="Variant effect data cache directory", required=True)
    print('Now parsing CLI arguments')
    cli_args = arg_parser.parse_args()
    files_glob = 'part-*'
    variant_cqs_glob = cli_args.cqs + files_glob
    variant_effects_glob = cli_args.effects + files_glob
    cqs_cache_dir = cli_args.cqs_cache_dir
    effects_cache_dir = cli_args.effects_cache_dir
    print('Variant CQS data: ', variant_cqs_glob)
    print('Variant effects data: ', variant_effects_glob)
    spark = SparkSession.builder.appName('hugecache').getOrCreate()
    cqs_selected = spark.read.json(variant_cqs_glob).select('varId', 'impact')
    cqs_filtered = cqs_selected.filter((cqs_selected.impact == 'HIGH') | (cqs_selected.impact == 'MODERATE'))
    inspect_df(cqs_filtered, "CQS cache")
    print('Now writing variants CQS cache to', cqs_cache_dir)
    cqs_filtered.write.mode('overwrite').json(cqs_cache_dir)
    print('Done writing variants CQS cache.')
    print('Now reading variant effects.')
    effects_selected = spark.read.json(variant_effects_glob).select('id', 'nearest')
    inspect_df(effects_selected, "variant effects selected")
    print('Now writing variants effects cache to', effects_cache_dir)
    effects_selected.write.mode('overwrite').json(effects_cache_dir)
    print('Done writing variants effects cache')
    print('Done with work, therefore stopping Spark')
    spark.stop()
    print('Spark stopped')


if __name__ == '__main__':
    main()
