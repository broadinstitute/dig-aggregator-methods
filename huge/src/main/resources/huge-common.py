import argparse
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, row_number, when, lit
from pyspark.sql.window import Window

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']
P_GWAS = 5e-8
PADDING = 100000


def get_variants(spark, variants_dir):
    unfiltered_variants = spark.read.json(variants_dir) \
        .select('varId', 'chromosome', 'position', 'pValue')
    return unfiltered_variants.filter(unfiltered_variants.pValue < P_GWAS)


def get_genes_gwas(variants, genes):
    variant_in_region = (genes.chromosomeGene == variants.chromosome) & \
                        (genes.start - PADDING <= variants.position) & \
                        (genes.end + PADDING >= variants.position)
    return genes \
        .join(variants, variant_in_region, "inner") \
        .select('varId', 'gene', 'pValue', 'ensembl')


def get_gene_causal(gene_gwas_cache):
    is_in_coding = (col('ensembl') == col('consequenceGeneId')) & \
                   ((col('consequenceImpact') == 'HIGH') |
                    (col('consequenceImpact') == 'MODERATE'))
    significant_by_gene = Window.partitionBy("gene").orderBy(col("pValue"))
    return gene_gwas_cache \
        .withColumn("row", row_number().over(significant_by_gene)) \
        .filter(col("row") == 1) \
        .drop("row") \
        .withColumnRenamed('varId', 'varIdCausal') \
        .withColumnRenamed('pValue', 'pValueCausal') \
        .withColumn('causalCoding', is_in_coding) \
        .withColumn('causalNearest', col('ensembl') == col('nearestGene')) \
        .withColumn('causalGWAS', lit(True)) \
        .drop('ensembl')


def get_gene_coding(gene_gwas_cache):
    is_in_coding = (col('ensembl') == col('consequenceGeneId')) & \
                   ((col('consequenceImpact') == 'HIGH') |
                    (col('consequenceImpact') == 'MODERATE'))
    return gene_gwas_cache \
        .filter(is_in_coding) \
        .select('gene') \
        .distinct() \
        .withColumn('locusGWASCoding', lit(True))


def join_dfs(genes, gene_causal, gene_coding):
    return genes \
        .join(gene_causal, ['gene'], 'left') \
        .join(gene_coding, ['gene'], 'left') \
        .withColumnRenamed('chromosomeGene', 'chromosome')


def main():
    arg_parser = argparse.ArgumentParser(prog='huge-common.py')
    arg_parser.add_argument("--phenotype", help="Phenotype (e.g. T2D) to run", required=True)
    args = arg_parser.parse_args()

    genes_dir = f'{s3_in}/out/huge/geneidmap/genes/'
    cache_dir = f'{s3_in}/out/huge/cache/'
    variants_dir = f'{s3_in}/out/metaanalysis/bottom-line/trans-ethnic/{args.phenotype}/'
    out_dir = f'{s3_out}/out/huge/common/{args.phenotype}'

    spark = SparkSession.builder.appName('huge-common').getOrCreate()
    genes = spark.read.json(genes_dir) \
        .select('chromosome', 'start', 'end', 'symbol', 'ensembl') \
        .withColumnRenamed('symbol', 'gene') \
        .withColumnRenamed('chromosome', 'chromosomeGene')

    variants = get_variants(spark, variants_dir)
    gene_gwas = get_genes_gwas(variants, genes)
    cache = spark.read.json(cache_dir) \
        .select('varId', 'consequenceImpact', 'consequenceGeneId', 'nearestGene')

    gene_gwas_cache = gene_gwas.join(cache, ['varId'], 'left')
    gene_gwas_cache.show()
    gene_causal = get_gene_causal(gene_gwas_cache)
    gene_coding = get_gene_coding(gene_gwas_cache)

    genes_joined = join_dfs(genes, gene_causal, gene_coding)
    genes_bf = genes_joined.withColumn(
        'bf_common',
        when(genes_joined.causalCoding, 350)
        .when(genes_joined.causalNearest, 45)
        .when(genes_joined.locusGWASCoding, 20)
        .when(genes_joined.causalGWAS, 3)
        .otherwise(1)
    ).withColumn('phenotype', lit(args.phenotype))

    genes_bf.write \
        .mode('overwrite') \
        .json(out_dir)
    spark.stop()


if __name__ == '__main__':
    main()
