import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # where to read input from
    variants_dir = 's3://dig-analysis-data/variant_counts/*/*/*/part-*'
    genes_dir = 's3://dig-analysis-data/genes/GRCh37/part-*'
    common_dir = 's3://dig-analysis-data/out/varianteffect/common/part-*'

    # where to write the output to
    outdir = f's3://dig-bio-index/variants/gene'

    # load all the variant counts
    variants = spark.read.json(variants_dir)

    # load all genes, keep only canonical symbol entries
    genes = spark.read.json(genes_dir)
    genes = genes.filter(genes.source == 'symbol') \
        .withColumnRenamed('name', 'gene')

    # load variant effects, keep only the pick=1 consequence
    common = spark.read.json(common_dir)

    # keep only variants overlapping genes
    overlap = (variants.chromosome == genes.chromosome) & \
        (variants.position >= genes.start) & \
        (variants.position < genes.end)

    # inner join genes with overlapped variants
    df = genes.join(variants, on=overlap, how='inner') \
        .drop(genes.chromosome)

    # join with common data per variant
    df = df.join(common, on='varId', how='left_outer')

    # index by position
    df.orderBy(['gene']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
