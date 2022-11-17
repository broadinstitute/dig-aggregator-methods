import argparse
from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--phenotype", help="The phenotype")
    arg_parser.add_argument("--genes", help="Gene data with regions")
    arg_parser.add_argument("--gene-associations", help="Gene data with p-values")
    arg_parser.add_argument("--variants", help="Variant data")
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
