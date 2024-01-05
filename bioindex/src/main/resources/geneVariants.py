from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, struct


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # where to read input from
    variants_dir = 's3://dig-analysis-data/variant_counts/*/*/*/part-00000*'
    genes_dir = 's3://dig-analysis-data/genes/GRCh37/part-*'
    cqs_dir = 's3a://dig-analysis-data/out/varianteffect/cqs/part-*'
    common_dir = 's3a://dig-analysis-data/out/varianteffect/common/part-*'

    # where to write the output to
    outdir = f's3://dig-bio-index/variants/gene'

    # load all the variant counts
    variants = spark.read.json(variants_dir)

    # load all genes, keep only canonical symbol entries
    genes = spark.read.json(genes_dir)
    genes = genes.filter(genes.source == 'symbol') \
        .withColumnRenamed('name', 'gene')

    # keep only variants overlapping genes
    overlap = (variants.chromosome == genes.chromosome) & \
        (variants.position >= genes.start) & \
        (variants.position < genes.end)

    # inner join genes with overlapped variants
    df = genes.join(variants, on=overlap, how='inner') \
        .drop(genes.chromosome)

    # join with common data per variant
    common = spark.read.json(common_dir)
    df = df.join(common, on='varId', how='left')

    # join with cqs data per variant (as list)
    cqs = spark.read.json(cqs_dir)
    col_struct = struct([c for c in cqs.columns if c not in ['varId']])
    cqs = cqs.groupBy('varId').agg(collect_list(col_struct).alias('vepRecords'))
    df = df.join(cqs, on='varId', how='left')

    # index by position
    df.repartition(100) \
        .orderBy(['gene']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
