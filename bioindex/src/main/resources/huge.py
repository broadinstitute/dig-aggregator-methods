import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    """
    Arguments: none
    """
    common_dir = f'{s3_in}/out/huge/common/*/part-*'
    rare_dir = f'{s3_in}/out/huge/rare/*/part-*'
    gene_outdir = f'{s3_bioindex}/huge/gene'
    phenotype_outdir = f'{s3_bioindex}/huge/phenotype'

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load the common data
    common_df = spark.read.json(common_dir) \
        .select('gene', 'phenotype', 'chromosome', 'start', 'end', 'bf_common')

    rare_df = spark.read.json(rare_dir) \
        .select('gene', 'phenotype', 'bf_rare')

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
