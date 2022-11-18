import argparse
from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    arg_parser = argparse.ArgumentParser(prog = 'huge.py')
    arg_parser.add_argument("--phenotype", help="The phenotype", required=True)
    arg_parser.add_argument("--genes", help="Gene data with regions", required=True)
    arg_parser.add_argument("--gene-associations", help="Gene data with p-values", required=True)
    arg_parser.add_argument("--variants", help="Variant data", required=True)
    cli_args = arg_parser.parse_args()
    phenotype = cli_args.phenotype
    genes_glob = cli_args.genes
    genes_assoc_glob = cli_args.gene_associations
    variants_glob = cli_args.variants
    print('Phenotype: ' + phenotype)
    print('Genes data with regions: ' + genes_glob)
    print('Gene data with p-values: ' + genes_assoc_glob)
    print('Variant data: ' + variants_glob)
    spark = SparkSession.builder.appName('huge').getOrCreate()
    spark.stop()


if __name__ == '__main__':
    main()
