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
        spark.read.json(genes_glob).select('chromosome', 'start', 'end', 'symbol', 'ensembl') \
            .withColumnRenamed('symbol', 'gene') \
            .withColumnRenamed('chromosome', 'chromosome_gene')
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
    variants_nearest_gene_list = \
        spark.read.json(variant_effects_glob).select('id', 'nearest') \
            .withColumnRenamed('id', 'varId').withColumnRenamed('nearest', 'nearest_list')
    variants_nearest_gene = \
        variants_nearest_gene_list.withColumn('nearest', col('nearest_list').getItem(0)) \
            .select('varId', 'nearest')
    inspect_df(variants_nearest_gene, "variants nearest gene")
    gene_locus_gaws_variant = genes.join(variants_gwas_cqs, genes.ensembl == variants_cqs.geneId)
    inspect_df(gene_locus_gaws_variant, "gene locus variant")
    gene_top_locus_variant = gene_locus_gaws_variant.withColumn("row", row_number().over(significant_by_gene)) \
        .filter(col("row") == 1).drop("row")
    inspect_df(gene_top_locus_variant, "gene top locus variant")
    top_locus_variant = gene_top_locus_variant \
        .withColumn("top_locus_variant_coding", (col('impact') == 'HIGH') | (col('impact') == 'MODERATE'))\
        .join(variants_nearest_gene, ['varId'], 'left') \
        .withColumnRenamed('varId', 'varId_locus_top').withColumnRenamed('pValue', 'pValue_locus_top')\
        .withColumn("top_locus_variant_nearest", col('gene') == col('nearest'))
    inspect_df(top_locus_variant, "top locus variant")
    genes_joined = \
        genes.join(gene_top_region_variant.select('gene', 'varId_region_top', 'pValue_region_top'), ['gene'], 'left') \
            .join(top_locus_variant.select('gene', 'varId_locus_top', 'pValue_locus_top',
                                           'top_locus_variant_coding', 'top_locus_variant_nearest'),
                  ['gene'], 'left') \
            .withColumnRenamed('chromosome_gene', 'chromosome')
    inspect_df(genes_joined, "genes joined")
    genes_bf = \
        genes_joined.withColumn('bf_common',
                                when(genes_joined.top_locus_variant_coding.isNotNull() &
                                     genes_joined.top_locus_variant_coding, 350)
                                .otherwise(when(genes_joined.top_locus_variant_nearest.isNotNull() &
                                                genes_joined.top_locus_variant_nearest, 45)
                                           .otherwise(when(genes_joined.varId_locus_top.isNotNull(), 20)
                                                      .otherwise(when(genes_joined.varId_region_top.isNotNull(), 3)
                                                                 .otherwise(1)))))
    inspect_df(genes_bf, "genes with BF")
    print('Now writing to ', out_dir)
    genes_bf.write.mode('overwrite').json(out_dir)
    print('Done with work, stopping Spark')
    spark.stop()
    print('Spark stopped')


if __name__ == '__main__':
    main()
