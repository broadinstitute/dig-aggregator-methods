import argparse
from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    print('Hello!')
    print('Now building argument parser')
    arg_parser = argparse.ArgumentParser(prog='huge.py')
    arg_parser.add_argument("--phenotype", help="The phenotype", required=True)
    arg_parser.add_argument("--genes", help="Gene data with regions", required=True)
    arg_parser.add_argument("--gene-associations", help="Gene data with p-values", required=True)
    arg_parser.add_argument("--variants", help="Variant data", required=True)
    arg_parser.add_argument("--padding", help="Variants are considered this far away from the gene", type=int,
                            default=100000)
    print('Now parsing CLI arguments')
    cli_args = arg_parser.parse_args()
    phenotype = cli_args.phenotype
    files_glob = 'part-*'
    genes_glob = cli_args.genes + files_glob
    genes_assoc_glob = cli_args.gene_associations + files_glob
    variants_glob = cli_args.variants + files_glob
    padding = cli_args.padding
    print('Phenotype: ' + phenotype)
    print('Genes data with regions: ' + genes_glob)
    print('Gene data with p-values: ' + genes_assoc_glob)
    print('Variant data: ' + variants_glob)
    print('Padding: ' + str(padding))
    spark = SparkSession.builder.appName('huge').getOrCreate()
    print('Genes from ' + genes_glob + ':')
    genes_regions_raw = spark.read.json(genes_glob)
    gene_regions = genes_regions_raw.select('chromosome', 'start', 'end', 'source', 'name') \
        .filter(genes_regions_raw.source == 'symbol').drop(genes_regions_raw.source)
    print('There are ' + str(gene_regions.count()) + ' gene regions:')
    for row in gene_regions.take(42):
        print(row)
    gene_p_values = spark.read.json(genes_assoc_glob).select('gene', 'pValue')
    print('There are ' + str(gene_p_values.count()) + ' gene associations')
    for row in gene_p_values.take(42):
        print(row)
    genes = gene_regions.join(gene_p_values, gene_regions.name == gene_p_values.gene).drop(gene_regions.name) \
        .withColumnRenamed('chromosome', 'chromosome_gene').withColumnRenamed('pValue', 'pValue_gene')
    print("Joined gene data gives " + str(genes.count()) + ' rows:')
    for row in genes.take(42):
        print(row)
    variants = spark.read.json(variants_glob).select('chromosome', 'position', 'reference', 'alt', 'pValue')
    print('There is data from ' + str(variants.count()) + ' variants:')
    for row in variants.take(42):
        print(row)
    cond = (genes.chromosome_gene == variants.chromosome) & \
           (genes.start - padding <= variants.position) & \
           (genes.end + padding >= variants.position)
    gene_variants = genes.join(variants.alias('variants'), cond, "inner").select('gene', 'pValue_gene', 'pValue')
    print('Joining genes and variants give ' + str(gene_variants.count()) + ' pairs:')
    for row in gene_variants.take(42):
        print(row)
    grouped = gene_variants.groupBy(gene_variants.gene, gene_variants.pValue_gene).agg({"pValue": "min"})
    for row in grouped.take(42):
        print(row)
    print('Stopping Spark')
    spark.stop()


if __name__ == '__main__':
    main()
