import argparse

from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    arg_parser = argparse.ArgumentParser(prog='gene-id-map.py')
    arg_parser.add_argument("--genes-dir", help="genes data dir", required=True)
    arg_parser.add_argument("--genes-out-dir", help="gene list output dir", required=True)
    cli_args = arg_parser.parse_args()
    files_glob = 'part-*'
    genes_glob = cli_args.genes_dir + files_glob
    genes_out_dir = cli_args.genes_out_dir
    spark = SparkSession.builder.appName('gene_id_map').getOrCreate()
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
    spark.stop()


if __name__ == '__main__':
    main()
