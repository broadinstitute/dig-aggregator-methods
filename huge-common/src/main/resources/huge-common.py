import argparse
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, when, array_contains


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
    arg_parser = argparse.ArgumentParser(prog='huge-common.py')
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
    p_gwas = 5e-8
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
    spark = SparkSession.builder.appName('huge-common').getOrCreate()
    # spark = SparkSession.builder.appName('huge') \
    #     .config('spark.driver.memory', '6g').config('spark.driver.maxResultSize', '2g').getOrCreate()
    genes = \
        spark.read.json(genes_glob).select('chromosome', 'start', 'end', 'symbol', 'ensemble') \
            .withColumnRenamed('symbol', 'gene')
    inspect_df(genes, "genes")
    variants = spark.read.json(variants_glob).select('varId', 'chromosome', 'position', 'pValue')
    inspect_df(variants, "variants for phenotype")
    variants_gwas = variants.filter(variants.pValue < p_gwas)
    inspect_df(variants_gwas, "GWAS variants for phenotype")
    variant_in_region = (genes.chromosome_gene == variants_gwas.chromosome) & \
                        (genes.start - padding <= variants_gwas.position) & \
                        (genes.end + padding >= variants_gwas.position)
    gene_variants_gwas = \
        genes.join(variants_gwas.alias('variants'), variant_in_region, "inner") \
            .select('varId', 'gene', 'pValue')
    inspect_df(gene_variants_gwas, "joined genes and variants")
    significant_by_gene = Window.partitionBy("gene").orderBy(col("pValue"))
    gene_top_region_variant = gene_variants_gwas.withColumn("row", row_number().over(significant_by_gene)) \
        .filter(col("row") == 1).drop("row") \
        .withColumnRenamed('varId', 'varId_region_top').withColumnRenamed('pValue', 'pValue_region_top')
    inspect_df(gene_top_region_variant, "genes with most significant GWAS variant")
    variants_cqs = spark.read.json(variant_cqs_glob).select('varId', 'impact', 'geneId')
    inspect_df(variants_cqs, "variants cqs")
    variants_gwas_cqs = variants_gwas.join(variants_cqs, ['varId'])
    inspect_df(variants_gwas_cqs, "GWAS variants cqs")
    gene_locus_gaws_variant = genes.join(variants_gwas_cqs, genes.ensembl == variants_cqs.geneId)
    inspect_df(gene_locus_gaws_variant, "gene locus variant")
    gene_locus_variant_impact = \
        gene_locus_gaws_variant.filter(gene_locus_gaws_variant.impact == 'HIGH' |
                                       gene_locus_gaws_variant.impact == 'MODERATE')
    inspect_df(gene_locus_variant_impact, "gene locus variant impact")
    # TODO
    variants_impact_raw = variants_cqs.filter((variants_cqs.impact == 'HIGH') | (variants_cqs.impact == 'MODERATE'))
    inspect_df(variants_impact_raw, "non-synonymous variants raw")
    variants_impact = variants_impact_raw.select('varId')
    inspect_df(variants_impact, "non-synonymous variants")
    variants_gwas_impact = variants_gwas.join(variants_impact, ['varId'])
    inspect_df(variants_gwas_impact, 'non-synonymous GWAS variants')
    gene_variants_gwas_impact = \
        genes.join(variants_gwas_impact.alias('variants'), variant_in_region, "inner") \
            .select('varId', 'gene', 'pValue')
    gene_top_impact_variant = gene_variants_gwas_impact.withColumn("row", row_number().over(significant_by_gene)) \
        .filter(col("row") == 1).drop("row") \
        .withColumnRenamed('varId', 'varId_top_impact').withColumnRenamed('pValue', 'pValue_top_impact_var')
    inspect_df(gene_top_impact_variant, 'genes with most significant GWAS non-synonymous variant')
    variant_effects = \
        spark.read.json(variant_effects_glob).select('id', 'nearest') \
            .withColumnRenamed('id', 'varId')
    inspect_df(variant_effects, "variant effects")
    gene_top_variant_nearest = \
        gene_top_region_variant.join(variant_effects, gene_top_region_variant.varId_top == variant_effects.varId)
    inspect_df(gene_top_variant_nearest, 'genes with most significant variant and its nearest gene')
    genes_joined = \
        genes.join(gene_top_variant_nearest, ['gene'], 'left').join(gene_top_impact_variant, ['gene'], 'left')
    inspect_df(genes_joined, 'genes with all relevant data')
    genes_flags = \
        genes_joined \
            .withColumn('has_gwas', genes_joined.varId_top.isNotNull()) \
            .withColumn('has_coding', genes_joined.varId_top_impact.isNotNull()) \
            .withColumn('is_nearest',
                        (genes_joined.nearest.isNotNull()) &
                        (array_contains(genes_joined.nearest, genes_joined.gene))) \
            .withColumn('has_causal_coding',
                        (genes_joined.varId_top.isNotNull()) & (genes_joined.varId_top_impact.isNotNull())
                        & (genes_joined.varId_top == genes_joined.varId_top_impact))
    inspect_df(genes_flags, "categories of genes")
    genes_all = genes_flags \
        .withColumn("bf_common",
                    when(genes_flags.has_causal_coding, 350)
                    .otherwise(when(genes_flags.is_nearest, 45)
                               .otherwise(when(genes_flags.has_coding, 20)
                                          .otherwise(when(genes_flags.has_gwas, 3)
                                                     .otherwise(1))))) \
        .select("gene", "chromosome_gene", "start", "end", "varId_top", "pValue_top_var", "varId_top_impact",
                "pValue_top_impact_var", "has_gwas", "has_coding", "is_nearest", "has_causal_coding", "bf_common") \
        .withColumnRenamed("varId", "varId_nearest") \
        .withColumnRenamed("chromosome_gene", "chromosome")
    inspect_df(genes_all, "final results for genes")
    print('Now writing to ', out_dir)
    genes_all.write.mode('overwrite').json(out_dir)
    print('Done with work, stopping Spark')
    spark.stop()
    print('Spark stopped')


if __name__ == '__main__':
    main()
