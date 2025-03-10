import os
from pyspark.sql import SparkSession, Row

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def to_dict(row, cols):
    return {k: row[k] for k in cols if k in row and row[k] is not None}


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # where to read input from
    variants_dir = f'{s3_in}/variant_counts/*/*/*/part-*'
    genes_dir = f's3://dig-analysis-bin/genes/GRCh37/part-*'
    cqs_dir = f'{s3_in}/out/varianteffect/variant_counts/cqs/part-*'
    common_dir = f'{s3_in}/out/varianteffect/variant_counts/common/part-*'

    # where to write the output to
    outdir = f'{s3_bioindex}/variants/{{}}'

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
    common = spark.read.json(common_dir) \
        .select('varId', 'dbSNP', 'consequence', 'nearest', 'minorAllele', 'maf', 'af')
    df = df.join(common, on='varId', how='left')

    # join with cqs data per variant (as list)
    cqs = spark.read.json(cqs_dir)
    cqs = cqs.drop('chromosome', 'position')

    cols = [c for c in cqs.columns if c not in ['varId']]
    flat_df = variants.join(cqs, on='varId', how='left')
    vep_records = flat_df.rdd \
        .keyBy(lambda r: r.varId) \
        .combineByKey(
            lambda row: [to_dict(row, cols)],
            lambda rows, row: rows + [to_dict(row, cols)],
            lambda rows1, rows2: rows1 + rows2
        ) \
        .map(lambda r: Row(
            varId=r[0],
            vepRecords=r[1]
        )) \
        .repartition(500) \
        .toDF()

    df = df.join(vep_records, on='varId', how='left')

    # index by position
    df.orderBy(['gene']) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('gene'))

    # index by position
    df.orderBy(['chromosome', 'position']) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('position'))

    # done
    spark.stop()


if __name__ == '__main__':
    main()
