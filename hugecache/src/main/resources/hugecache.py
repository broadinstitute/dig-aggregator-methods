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
    arg_parser.add_argument("--genes", help="Gene data with regions", required=True)
    arg_parser.add_argument("--cqs", help="Variant CQS data", required=True)
    arg_parser.add_argument("--effects", help="Variant effect data", required=True)
    print('Now parsing CLI arguments')
    cli_args = arg_parser.parse_args()
    files_glob = 'part-*'
    genes_glob = cli_args.genes + files_glob
    variant_cqs_glob = cli_args.cqs + files_glob
    variant_effects_glob = cli_args.effects + files_glob
    print('Genes data with regions: ', genes_glob)
    print('Variant CQS data: ', variant_cqs_glob)
    print('Variant effects data: ', variant_effects_glob)
    spark = SparkSession.builder.appName('huge').getOrCreate()
    genes_regions_raw = spark.read.json(genes_glob)
    gene_regions = genes_regions_raw.select('chromosome', 'start', 'end', 'source', 'name') \
        .filter(genes_regions_raw.source == 'symbol').drop(genes_regions_raw.source)
    inspect_df(gene_regions, "gene regions")
    variant_cqs = spark.read.json(variant_cqs_glob) \
        .select('varId', 'consequenceTerms', 'distance', 'geneId', 'geneSymbol', 'impact', 'pick')
    inspect_df(variant_cqs, "CQS")
    variant_effects = spark.read.json(variant_effects_glob).select('id', 'nearest')
    inspect_df(variant_effects, "variant effects")
    print('Stopping Spark')
    spark.stop()


if __name__ == '__main__':
    main()
