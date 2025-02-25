import os

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, row_number, greatest, lit

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']
PADDING = 2000000


def get_joined_df(genes, variants):
    variant_in_region = \
        (genes.chromosome_gene == variants.chromosome) & \
        (genes.start - PADDING <= variants.position) & \
        (genes.end + PADDING >= variants.position)
    return genes.join(variants, variant_in_region) \
        .drop("chromosome_gene")


def get_nearest_df(joined_df):
    distances = joined_df \
        .withColumn("distance", greatest(col("start") - col("position"), col("position") - col("end"), lit(0))) \
        .withColumn("length", col("end") - col("start"))
    distances_by_gene = Window.partitionBy("varId") \
        .orderBy(col("distance"), col("length"))
    return distances \
        .withColumn("row", row_number().over(distances_by_gene)) \
        .filter(col("row") == 1) \
        .drop("row")


def main():
    genes_dir = f'{s3_in}/out/huge/geneidmap/genes'
    variants_dir = f'{s3_in}/out/varianteffect/variants/common'
    out_dir = f'{s3_out}/out/huge/nearestgenes/'

    spark = SparkSession.builder.appName('nearest_gene').getOrCreate()
    genes = spark.read.json(f'{genes_dir}/part-*') \
        .select("chromosome", "start", "end", "ensembl") \
        .withColumnRenamed("chromosome", "chromosome_gene")
    variants = spark.read.json(f'{variants_dir}/part-*') \
        .select("varId", "chromosome", "position")

    joined_df = get_joined_df(genes, variants)
    nearest_df = get_nearest_df(joined_df)

    nearest_df \
        .select("varId", "ensembl") \
        .withColumnRenamed("ensembl", "nearestGene") \
        .write.mode('overwrite') \
        .json(out_dir)
    spark.stop()


if __name__ == '__main__':
    main()
