import argparse
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
import subprocess

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


@udf(returnType=StringType())
def symbol_to_ensembl(symbol: str) -> str:
    cmd = ('eugene', 'util', 'symbol-to-gene-id', '-s', symbol)
    process = subprocess.run(cmd, capture_output=True, text=True)
    ensembl = process.stdout.strip()
    print("Mapped symbol", symbol, "to Ensembl id ", ensembl)
    return ensembl


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
    arg_parser = argparse.ArgumentParser(prog='gene-id-map.py')
    arg_parser.add_argument("--genes-dir", help="genes data dir", required=True)
    arg_parser.add_argument("--variant-effects-dir", help="variant effects data dir", required=True)
    arg_parser.add_argument("--genes-out-dir", help="gene list output dir", required=True)
    arg_parser.add_argument("--map-dir", help="gene id mapping output dir", required=True)
    print('Now parsing CLI arguments')
    cli_args = arg_parser.parse_args()
    files_glob = 'part-*'
    genes_glob = cli_args.genes_dir + files_glob
    variant_effects_glob = cli_args.variant_effects_dir + files_glob
    genes_out_dir = cli_args.genes_out_dir
    map_dir = cli_args.map_dir
    print('Gene data dir: ', genes_glob)
    print('Variant effects data dir: ', variant_effects_glob)
    print('Id map output dir: ', map_dir)
    spark = SparkSession.builder.appName('geneidmap').getOrCreate()
    genes = spark.read.json(genes_glob).select("chromosome", "start", "end", "name", "source")
    genes_symbol = \
        genes.filter(genes.source == "symbol") \
            .select("chromosome", "start", "end", "name") \
            .withColumnRenamed("name", "symbol")
    genes_ensembl = \
        genes.filter(genes.source == "ensembl") \
            .select("chromosome", "start", "end", "name") \
            .withColumnRenamed("name", "ensembl")
    genes_joined = genes_symbol.join(genes_ensembl, ["chromosome", "start", "end"])
    genes_joined.write.mode("overwrite").json(genes_out_dir)
    symbols = \
        spark.read.json(variant_effects_glob).select("nearest") \
            .withColumn("symbol", col("nearest").getItem(0)).select("symbol").distinct()
    inspect_df(symbols, "Nearest from effects")
    gene_id_map = symbols.withColumn("ensembl", symbol_to_ensembl(symbols.symbol))
    inspect_df(gene_id_map, "gene id map")
    gene_id_map.write.mode("overwrite").json(map_dir)
    print('Done with work, therefore stopping Spark')
    spark.stop()
    print('Spark stopped')


if __name__ == '__main__':
    main()
