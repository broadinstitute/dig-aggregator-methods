import argparse
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, when


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
    arg_parser.add_argument("--variants", help="Variant data", required=True)
    arg_parser.add_argument("--padding", help="Variants are considered this far away from the gene", type=int,
                            default=100000)
    arg_parser.add_argument("--cqs", help="Variant CQS data", required=True)
    arg_parser.add_argument("--effects", help="Variant effect data", required=True)
    arg_parser.add_argument("--out-dir", help="Output directory", required=True)

    print('Now parsing CLI arguments')
    cli_args = arg_parser.parse_args()
    phenotype = cli_args.phenotype
    files_glob = 'part-*'
    p_significant = 5e-8
    genes_glob = cli_args.genes + files_glob
    variants_glob = cli_args.variants + files_glob
    variant_cqs_glob = cli_args.cqs + files_glob
    variant_effects_glob = cli_args.effects + files_glob
    padding = cli_args.padding
    out_dir = cli_args.out_dir
    print('Phenotype: ', phenotype)
    print('Genes data with regions: ', genes_glob)
    print('Variant data: ', variants_glob)
    print('Variant CQS data: ', variant_cqs_glob)
    print('Variant effects data: ', variant_effects_glob)
    print('Padding: ', padding)
    print('Output directory: ', out_dir)
    spark = SparkSession.builder.appName('huge').getOrCreate()
    # spark = SparkSession.builder.appName('huge') \
    #     .config('spark.driver.memory', '6g').config('spark.driver.maxResultSize', '2g').getOrCreate()
    genes_regions_raw = spark.read.json(genes_glob)
    genes = genes_regions_raw.select('chromosome', 'start', 'end', 'source', 'name') \
        .filter(genes_regions_raw.source == 'symbol')\
        .drop(genes_regions_raw.source).withColumnRenamed('symbol', 'gene')
    inspect_df(genes, "genes")
    variants = spark.read.json(variants_glob).select('varId', 'chromosome', 'position', 'pValue')
    inspect_df(variants, "variants for phenotype")
    variants_gwas = variants.filter(variants.pValue < p_significant)
    inspect_df(variants_gwas, "GWAS variants for phenotype")
    variant_in_locus = (genes.chromosome_gene == variants_gwas.chromosome) & \
                       (genes.start - padding <= variants_gwas.position) & \
                       (genes.end + padding >= variants_gwas.position)
    gene_variants_gwas = \
        genes.join(variants_gwas.alias('variants'), variant_in_locus, "inner") \
            .select('varId', 'gene', 'pValue')
    inspect_df(gene_variants_gwas, "joined genes and variants")
    significant_by_gene = Window.partitionBy("gene").orderBy(col("pValue"))
    gene_top_variant = gene_variants_gwas.withColumn("row", row_number().over(significant_by_gene)) \
        .filter(col("row") == 1).drop("row") \
        .withColumnRenamed('varId', 'varId_top').withColumnRenamed('pValue', 'pValue_top_var')
    inspect_df(gene_top_variant, "genes with most significant GWAS variant")
    variants_cqs = spark.read.json(variant_cqs_glob).select('varId', 'impact')
    variants_impact_raw = variants_cqs.filter((variants_cqs.impact == 'HIGH') | (variants_cqs.impact == 'MODERATE'))
    inspect_df(variants_impact_raw, "non-synonymous variants raw")
    variants_impact = variants_impact_raw.select('varId')
    inspect_df(variants_impact, "non-synonymous variants")
    variants_gwas_impact = variants_gwas.join(variants_impact, ['varId'])
    inspect_df(variants_gwas_impact, 'non-synonymous GWAS variants')
    gene_variants_gwas_impact = \
        genes.join(variants_gwas_impact.alias('variants'), variant_in_locus, "inner") \
            .select('varId', 'gene', 'pValue_gene', 'pValue', 'bf_rare')
    gene_top_impact_variant = gene_variants_gwas_impact.withColumn("row", row_number().over(significant_by_gene)) \
        .filter(col("row") == 1).drop("row") \
        .withColumnRenamed('varId', 'varId_top_impact').withColumnRenamed('pValue', 'pValue_top_impact_var')
    inspect_df(gene_top_impact_variant, 'genes with most significant GWAS non-synonymous variant')
    variant_effects = \
        spark.read.json(variant_effects_glob).select('id', 'nearest') \
            .withColumnRenamed('id', 'varId')
    inspect_df(variant_effects, "variant effects")
    gene_top_variant_nearest = \
        gene_top_variant.join(variant_effects, gene_top_variant.varId_top == variant_effects.varId)
    inspect_df(gene_top_variant_nearest, 'genes with most significant variant and its nearest gene')
    genes_joined = \
        genes.join(gene_top_variant_nearest, ['gene'], 'left').join(gene_top_impact_variant, ['gene'], 'left')
    inspect_df(genes_joined, 'genes with all relevant data')
    genes_flags = \
        genes_joined \
            .withColumn('has_gwas', genes_joined.varId_top is not None) \
            .withColumn('has_coding', genes_joined.varId_top_impact is not None) \
            .withColumn('is_nearest',
                        (genes_joined.nearest is not None) & (genes_joined.gene in genes_joined.nearest)) \
            .withColumn('has_causal_coding',
                        (genes_joined.varId_top is not None) & (genes_joined.varId_top_impact is not None)
                        & (genes_joined.varId_top == genes_joined.varId_top_impact))
    inspect_df(genes_flags, "categories of genes")
    genes_all = genes_flags\
        .withColumn("bf_common",
                    when(genes_flags.has_causal_coding, 350)
                    .otherwise(when(genes_flags.is_nearest, 45)
                               .otherwise(when(genes_flags.has_coding, 20)
                                          .otherwise(when(genes_flags.has_gwas, 3)
                                                     .otherwise(1)))))
    inspect_df(genes_all, "final results for genes")
    print('Now writing to ', out_dir)
    genes_all.write.mode('overwrite').json(out_dir)
    print('Done with work, stopping Spark')
    spark.stop()
    print('Spark stopped')


if __name__ == '__main__':
    main()
