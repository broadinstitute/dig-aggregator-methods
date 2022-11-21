import argparse
from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    arg_parser = argparse.ArgumentParser(prog='huge.py')
    arg_parser.add_argument("--phenotype", help="The phenotype", required=True)
    arg_parser.add_argument("--genes", help="Gene data with regions", required=True)
    arg_parser.add_argument("--gene-associations", help="Gene data with p-values", required=True)
    arg_parser.add_argument("--variants", help="Variant data", required=True)
    cli_args = arg_parser.parse_args()
    phenotype = cli_args.phenotype
    files_glob = 'part-*'
    genes_glob = cli_args.genes + files_glob
    genes_assoc_glob = cli_args.gene_associations + files_glob
    variants_glob = cli_args.variants + files_glob
    print('Phenotype: ' + phenotype)
    print('Genes data with regions: ' + genes_glob)
    print('Gene data with p-values: ' + genes_assoc_glob)
    print('Variant data: ' + variants_glob)
    spark = SparkSession.builder.appName('huge').getOrCreate()
    print('Content of ' + genes_glob + ':')
    for row in spark.read.json(genes_glob).take(42):
        print(row)
    print('Stopping Spark')
    spark.stop()


if __name__ == '__main__':
    main()
