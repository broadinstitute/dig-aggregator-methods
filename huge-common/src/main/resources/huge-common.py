import argparse
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, row_number, when
from pyspark.sql.window import Window


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
    arg_parser.add_argument("--cache", help="Cache data", required=True)
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
    cache_glob = cli_args.cache + files_glob
    variant_cqs_glob = cli_args.cqs + files_glob
    variant_effects_glob = cli_args.effects + files_glob
    padding = cli_args.padding
    out_dir = cli_args.out_dir
    print('Phenotype: ', phenotype)
    print('Genes data with regions: ', genes_glob)
    print('Variant data: ', variants_glob)
    print('Cache data: ', cache_glob)
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
    gene_gwas = \
        genes.join(variants_gwas.alias('variants'), variant_in_region, "inner") \
            .select('varId', 'gene', 'pValue')
    inspect_df(gene_gwas, "joined genes and variants")
    cache = spark.read.json(cache_glob).select('varId', 'impact', 'geneId', 'nearest_ensembl')
    inspect_df(cache, "cache")
    gene_gwas_cache = gene_gwas.join(cache, ['varId'], 'left')
    significant_by_gene = Window.partitionBy("gene").orderBy(col("pValue"))
    is_coding = (col('impact') == 'HIGH') | (col('impact') == 'MODERATE')
    gene_causal = gene_gwas_cache.withColumn("row", row_number().over(significant_by_gene)) \
        .filter(col("row") == 1).drop("row") \
        .withColumnRenamed('varId', 'varId_causal').withColumnRenamed('pValue', 'pValue_causal') \
        .withColumn('causal_coding', is_coding)\
        .withColumn('causal_nearest', col('ensembl') == col('geneId'))\
        .withColumn('causal_gwas', True)
    inspect_df(gene_causal, "genes with causal variant")
    gene_coding = \
        gene_gwas_cache.filter((col('ensemble') == col('geneId')) & is_coding).select('gene').distinct() \
            .withColumn('locus_gaws_coding', True)
    genes_joined = genes.join(gene_causal, ['gene'], 'left').join(gene_coding, ['gene'], 'left')
    inspect_df(genes_joined, "genes joined")
    genes_bf = \
        genes_joined.withColumn('bf_common',
                                when(genes_joined.causal_coding, 350)
                                .when(genes_joined.causal_nearest, 45)
                                .when(genes_joined.locus_gaws_coding, 20)
                                .when(genes_joined.causal_gwas, 3)
                                .otherwise(1))
    inspect_df(genes_bf, "genes with BF")
    print('Now writing to ', out_dir)
    genes_bf.write.mode('overwrite').json(out_dir)
    print('Done with work, stopping Spark')
    spark.stop()
    print('Spark stopped')


if __name__ == '__main__':
    main()
