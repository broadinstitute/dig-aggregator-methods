import argparse
from pyspark.sql import SparkSession
from datetime import datetime


def now_str():
    return str(datetime.now())


def main():
    """
    Arguments: none
    """
    print('Hello! The time is now ', now_str())
    print('Now building argument parser')
    arg_parser = argparse.ArgumentParser(prog='gene-id-map.py')
    arg_parser.add_argument("--genes-dir", help="genes data dir", required=True)
    arg_parser.add_argument("--map-dir", help="gene id mapping output dir", required=True)
    print('Now parsing CLI arguments')
    cli_args = arg_parser.parse_args()
    files_glob = 'part-*'
    genes_glob = cli_args.genes_dir + files_glob
    map_dir = cli_args.map_dir
    print('Gene data dir: ', genes_glob)
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
    genes_joined.write.mode("overwrite").json(map_dir)
    print('Done with work, therefore stopping Spark')
    spark.stop()
    print('Spark stopped')


if __name__ == '__main__':
    main()
