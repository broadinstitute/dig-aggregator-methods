import os
from pyspark.sql import SparkSession, DataFrame

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def main():
    variants_common_dir = f'{s3_in}/out/varianteffect/common'
    nearest_gene_dir = f'{s3_in}/out/huge/nearestgenes'
    out_dir = f'{s3_out}/out/huge/cache'

    spark = SparkSession.builder.appName('huge-cache').getOrCreate()

    common_cache = spark.read \
        .json(f'{variants_common_dir}/part-*') \
        .select('varId', 'consequenceImpact', 'consequenceGeneId')
    nearest_genes_cache = spark.read.json(f'{nearest_gene_dir}/part-*') \
        .select("varId", "nearestGene")

    cache = common_cache.join(nearest_genes_cache, ["varId"], "outer")
    cache.write \
        .mode('overwrite') \
        .json(out_dir)
    spark.stop()


if __name__ == '__main__':
    main()
