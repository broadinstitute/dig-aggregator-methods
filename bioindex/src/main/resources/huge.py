import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, udf

OUT_BUCKET = f'dig-bio-index'


def main():
    """
    Arguments: none
    """
    common_dir = f's3://dig-analysis-data/out/huge/common/*/part-*'
    rare_dir = f's3://dig-analysis-data/out/huge/rare/*/part-*'
    gene_outdir = f's3://{OUT_BUCKET}/huge/gene'
    phenotype_outdir = f's3://{OUT_BUCKET}/huge/phenotype'

    # pull out phenotype from input file name (for either common or rare)
    phenotype_of_source = udf(lambda s: s and re.search(src_re, s).group(2))
    src_re = r'/out/huge/([^/]+)/([^/]+)/'

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load the common data
    common_df = spark.read.json(common_dir) \
        .withColumn('source', input_file_name()) \
        .select(
            col('gene'),
            phenotype_of_source('source').alias('phenotype'),
            col('chromosome_gene').alias('chromosome'),
            col('start'),
            col('end'),
            col('bf_common')
        )

    rare_df = spark.read.json(rare_dir) \
        .withColumn('source', input_file_name()) \
        .select(
            col('gene'),
            phenotype_of_source('source').alias('phenotype'),
            col('bf_rare')
        )

    df = common_df.join(rare_df, on=['gene', 'phenotype'], how='left')

    # If rare is missing fill in with bf_rare = 1
    df = df.fillna({'bf_rare': 1.0})

    # Add in huge value
    df = df.withColumn('huge', df.bf_common * df.bf_rare)

    # index by gene
    df.orderBy(col("gene").asc(), col("huge").desc()) \
        .write \
        .mode('overwrite') \
        .json(gene_outdir)

    # index by
    df.orderBy(col("phenotype").asc(), col("huge").desc()) \
        .write \
        .mode('overwrite') \
        .json(phenotype_outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
