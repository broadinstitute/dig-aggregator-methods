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
    arg_parser.add_argument("--phenotype", help="The phenotype", required=True)
    arg_parser.add_argument("--genes", help="Gene data with regions", required=True)
    arg_parser.add_argument("--gene-associations", help="Gene data with p-values", required=True)
    arg_parser.add_argument("--variants", help="Variant data", required=True)
    arg_parser.add_argument("--padding", help="Variants are considered this far away from the gene", type=int,
                            default=100000)
    arg_parser.add_argument("--cqs", help="Variant CQS data", required=True)
    arg_parser.add_argument("--effects", help="Variant effect data", required=True)
    print('Now parsing CLI arguments')
    cli_args = arg_parser.parse_args()
    phenotype = cli_args.phenotype
    files_glob = 'part-*'
    p_significant = 5e-8
    genes_glob = cli_args.genes + files_glob
    genes_assoc_glob = cli_args.gene_associations + files_glob
    variants_glob = cli_args.variants + files_glob
    variant_cqs_glob = cli_args.cqs + files_glob
    variant_effects_glob = cli_args.effects + files_glob
    padding = cli_args.padding
    print('Phenotype: ', phenotype)
    print('Genes data with regions: ', genes_glob)
    print('Gene data with p-values: ', genes_assoc_glob)
    print('Variant data: ', variants_glob)
    print('Variant CQS data: ', variant_cqs_glob)
    print('Variant effects data: ', variant_effects_glob)
    print('Padding: ', padding)
    spark = SparkSession.builder.appName('huge').getOrCreate()
    genes_regions_raw = spark.read.json(genes_glob)
    gene_regions = genes_regions_raw.select('chromosome', 'start', 'end', 'source', 'name') \
        .filter(genes_regions_raw.source == 'symbol').drop(genes_regions_raw.source)
    inspect_df(gene_regions, "gene regions")
    gene_p_values = spark.read.json(genes_assoc_glob).select('gene', 'pValue')
    inspect_df(gene_p_values, "gene associations")
    genes = gene_regions.join(gene_p_values, gene_regions.name == gene_p_values.gene).drop(gene_regions.name) \
        .withColumnRenamed('chromosome', 'chromosome_gene').withColumnRenamed('pValue', 'pValue_gene')
    inspect_df(genes, "joined genes data")
    variants = spark.read.json(variants_glob).select('varId', 'chromosome', 'position', 'pValue')
    inspect_df(variants, "variants for phenotype")
    variants_gwas = variants.filter(variants.pValue < p_significant)
    inspect_df(variants_gwas, "GWAS variants for phenotype")
    cond = (genes.chromosome_gene == variants_gwas.chromosome) & \
           (genes.start - padding <= variants_gwas.position) & \
           (genes.end + padding >= variants_gwas.position)
    gene_variants_gwas = \
        genes.join(variants_gwas.alias('variants'), cond, "inner").select('varId', 'gene', 'pValue_gene', 'pValue')
    inspect_df(gene_variants_gwas, "joined genes and variants")
    genes_gwas_counts = \
        gene_variants_gwas.groupBy(gene_variants_gwas.gene, gene_variants_gwas.pValue_gene).count()
    inspect_df(genes_gwas_counts, "genes with number of GWAS variants")
    genes_gwas = genes_gwas_counts.filter(genes_gwas_counts.colRegex('count') > 0).select('gene', 'pValue_gene')
    inspect_df(genes_gwas, "GWAS genes")
    variants_cqs = spark.read.json(variant_cqs_glob).select('varId', 'impact')
    variants_causal_raw = variants_cqs.filter((variants_cqs.impact == 'HIGH') | (variants_cqs.impact == 'MODERATE'))
    inspect_df(variants_causal_raw, "causal variants raw")
    variants_causal = variants_causal_raw.select('varId')
    inspect_df(variants_causal, "causal variants")
    variant_effects = spark.read.json(variant_effects_glob).select('id', 'nearest')
    inspect_df(variant_effects, "variant effects")
    variants_gwas_causal = variants_gwas.join(variants_causal, ['varId'])
    inspect_df(variants_gwas_causal, "causal GWAS variants")
    print('Done with work implemented so far, stopping Spark')
    spark.stop()
    print('Spark stopped')


if __name__ == '__main__':
    main()
