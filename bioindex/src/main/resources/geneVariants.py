import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# what bucket will be output to?
OUT_BUCKET = f'dig-bio-{"test" if os.getenv("JOB_DRYRUN") else "index"}'


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # where to read input from
    genes_dir = f's3://dig-analysis-data/genes/GRCh37/part-*'
    common_dir = f's3://dig-analysis-data/out/varianteffect/common/part-*'

    # where to write the output to
    outdir = f's3://{OUT_BUCKET}/variants/gene'

    # load all genes, keep only canonical symbol entries
    genes = spark.read.json(genes_dir)
    genes = genes.filter(genes.source == 'symbol') \
        .withColumnRenamed('name', 'gene')

    # load common variant data
    common = spark.read.json(common_dir)

    # is the variant overlapping the gene?
    overlap = (common.chromosome == genes.chromosome) & \
        (common.position >= genes.start) & \
        (common.position < genes.end)

    # inner join genes with overlapped variants
    df = genes.join(common, on=overlap, how='inner') \
        .drop(genes.chromosome)  # duplicate column in common frame

    # index by position
    df.orderBy(['gene']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
