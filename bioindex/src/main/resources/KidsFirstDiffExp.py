import numpy as np
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, BooleanType
from pyspark.sql.functions import input_file_name, lit, rank, udf, when
from pyspark.sql.window import Window

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']
raw_path = '/mnt/var/raw'

SCHEMA = StructType(
    [
        StructField('geneName', StringType()),
        StructField('meanExpression', DoubleType()),
        StructField('log2FoldChange', DoubleType()),
        StructField('log2FoldChangeSE', DoubleType()),
        StructField('Statistic', DoubleType()),
        StructField('pValue', DoubleType()),
        StructField('adjustedPValue', DoubleType())
    ]
)


def get_biosample_map():
    biosamples = {}
    with open(f'{raw_path}/tissue_map.tsv', 'r') as f:
        for line in f:
            biosample, portal_name = line.strip().split('\t')
            portal_tissue, portal_biosample = portal_name.strip().split('___')
            biosamples[biosample] = (portal_tissue, portal_biosample)
    return biosamples


def get_phenotype_map():
    phenotypes = {}
    with open(f'{raw_path}/phenotype_map.tsv', 'r') as f:
        for line in f:
            split_line = line.strip().split('\t')
            if len(split_line) == 3:
                phenotype, name, mondo = split_line
                phenotypes[phenotype] = (name, mondo)
    return phenotypes


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = f'{s3_in}/tissue-sumstats/*.out'
    outdir = f'{s3_bioindex}/diff_exp/{{}}/kidsfirst/'

    df = spark.read.csv(srcdir, header=True, sep='\t', schema=SCHEMA) \
        .withColumn('source', lit('KidsFirst'))

    tissue_of_input = udf(lambda s: re.search(r'.*/([^\./]+).([^\./]+).sort.filter.out', s).group(1))
    phenotype_of_input = udf(lambda s: re.search(r'.*/([^\./]+).([^\./]+).sort.filter.out', s).group(2).lower())

    # extract the dataset and ancestry from the filename
    df = df.withColumn('filePhenotype', phenotype_of_input(input_file_name())) \
        .withColumn('fileTissue', tissue_of_input(input_file_name()))

    # pValues can be too small for strings
    df = df.withColumn('pValue', when(df.pValue == 0.0, np.nextafter(0, 1)).otherwise(df.pValue))

    phenotype_map = get_phenotype_map()
    biosample_map = get_biosample_map()
    tissue_filter = udf(lambda s: s in biosample_map, BooleanType())
    phenotype_filter = udf(lambda s: s in phenotype_map, BooleanType())
    apply_tissue_map = udf(lambda s: biosample_map[s][0])
    apply_biosample_map = udf(lambda s: biosample_map[s][1])
    apply_mondo_map = udf(lambda s: phenotype_map[s][0])
    apply_name_map = udf(lambda s: phenotype_map[s][1])
    df = df.filter((tissue_filter(df.gtexTissue)) & (phenotype_filter(df.filePhenotype))) \
        .withColumn('tissue', apply_tissue_map(df.fileTissue)) \
        .withColumn('biosample', apply_biosample_map(df.fileTissue)) \
        .withColumn('phenotype', apply_mondo_map(df.filePhenotype)) \
        .withColumn('phenotype_name', apply_name_map(df.filePhenotype)) \
        .drop('filePhenotype', 'fileTissue')

    df.orderBy(['geneName', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('gene'))

    df.orderBy(['phenotype', 'tissue', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('phenotype-tissue'))

    phenotype_partition = Window.partitionBy('phenotype').orderBy('pValue')
    phenotype_df = df.withColumn('rank', rank().over(phenotype_partition))
    phenotype_df \
        .filter(phenotype_df.rank <= 10000) \
        .drop('rank') \
        .orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('phenotype'))

    phenotype_df \
        .filter(phenotype_df.rank <= 20) \
        .drop('rank') \
        .withColumn('dummy', lit(1)) \
        .orderBy(['pValue']) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('top-phenotype'))

    tissue_partition = Window.partitionBy('tissue').orderBy('pValue')
    tissue_df = df.withColumn('rank', rank().over(tissue_partition))
    tissue_df \
        .filter(tissue_df.rank <= 10000) \
        .drop('rank') \
        .orderBy(['tissue', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('tissue'))

    tissue_df \
        .filter(tissue_df.rank <= 20) \
        .drop('rank') \
        .withColumn('dummy', lit(1)) \
        .orderBy(['pValue']) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('top-tissue'))

    # done
    spark.stop()


if __name__ == '__main__':
    main()

