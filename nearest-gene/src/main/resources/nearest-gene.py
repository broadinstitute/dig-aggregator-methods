import argparse
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
import subprocess
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import StringType


def symbol_to_ensembl_impl(symbol: str) -> str:
    cmd = ('eugene', 'util', 'symbol-to-gene-id', '-s', symbol)
    process = subprocess.run(cmd, capture_output=True, text=True)
    ensembl = process.stdout.strip()
    print("Mapped symbol", symbol, "to Ensembl id ", ensembl)
    return ensembl


symbol_to_ensembl = udf(lambda symbol: symbol_to_ensembl_impl(symbol), StringType()).asNondeterministic()


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
    arg_parser.add_argument("--out-dir", help="output dir", required=True)
    print('Now parsing CLI arguments')
    cli_args = arg_parser.parse_args()
    files_glob = 'part-*'
    genes_glob = cli_args.genes_dir + files_glob
    variant_effects_glob = cli_args.variant_effects_dir + files_glob
    out_dir = cli_args.genes_out_dir
    print('Gene data dir: ', genes_glob)
    print('Variant effects data dir: ', variant_effects_glob)
    print('Output dir: ', out_dir)
    spark = SparkSession.builder.appName('nearestgene').getOrCreate()
    # genes = spark.read.json(genes_glob).select("chromosome", "start", "end", "name", "source")
    # genes_symbol = \
    #     genes.filter(genes.source == "symbol") \
    #         .select("chromosome", "start", "end", "name") \
    #         .withColumnRenamed("name", "symbol")
    # genes_ensembl = \
    #     genes.filter(genes.source == "ensembl") \
    #         .select("chromosome", "start", "end", "name") \
    #         .withColumnRenamed("name", "ensembl")
    # genes_joined = genes_symbol.join(genes_ensembl, ["chromosome", "start", "end"])
    # genes_joined.write.mode("overwrite").json(genes_out_dir)
    symbols = \
        spark.read.json(variant_effects_glob).select("nearest") \
            .withColumn("symbol", col("nearest").getItem(0)).select("symbol").distinct()
    inspect_df(symbols, "Nearest from effects")
    gene_id_map = symbols.withColumn("ensembl", symbol_to_ensembl(symbols.symbol)).persist()
    inspect_df(gene_id_map, "gene id map")
    count = 1
    print("Entering loop")
    while count > 0:
        gene_id_map_missing = gene_id_map.filter(col("ensembl") == "").persist()
        inspect_df(gene_id_map_missing, "gene id map missing")
        count = gene_id_map_missing.count()
        print("count is", count, "and time is", now_str())
        gene_id_map_missing.unpersist()
        gene_id_map_new = \
            gene_id_map.withColumn("ensembl_new",
                                   when(gene_id_map.ensembl == "", symbol_to_ensembl(gene_id_map.symbol))
                                   .otherwise(gene_id_map.ensembl)) \
                .drop("ensembl").withColumnRenamed("ensembl_new", "ensembl").persist()
        gene_id_map.unpersist()
        gene_id_map = gene_id_map_new

    inspect_df(gene_id_map, "gene id map")
    print("Exited the loop, now writing gene id map.")
    gene_id_map.write.mode("overwrite").json(map_dir)
    print('Done with work, therefore stopping Spark')
    spark.stop()
    print('Spark stopped')


if __name__ == '__main__':
    main()
