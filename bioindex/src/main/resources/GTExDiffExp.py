import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, BooleanType
from pyspark.sql.functions import input_file_name, lit, rank, udf
from pyspark.sql.window import Window

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']
raw_path = '/mnt/var/raw'

SCHEMA = StructType(
    [
        StructField('gene', StringType()),
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
    with open(f'{raw_path}/gtex_to_uberon.tsv', 'r') as f:
        for line in f:
            split_line = line.strip().split('\t')
            if len(split_line) > 5 and '___' in split_line[5]:
                biosample = split_line[0].strip()
                portal_tissue, portal_biosample = split_line[5].strip().split('___')
                biosamples[biosample] = (portal_tissue, portal_biosample)
    return biosamples


def get_phenotype_map():
    phenotypes = {}
    with open(f'{raw_path}/phenotype_to_mondo.tsv', 'r') as f:
        f.readline()
        for line in f:
            split_line = line.strip().split('\t')
            if len(split_line) >= 6 and len(split_line[0]) > 0 and len(split_line[5]) > 0:
                phenotype = split_line[0].strip().lower()
                mondo = split_line[5].strip()
                phenotypes[phenotype] = mondo
    # temporary filter
    mondo_counts = {}
    for phenotype, mondo in phenotypes.items():
        if mondo not in mondo_counts:
            mondo_counts[mondo] = 0
        mondo_counts[mondo] += 1
    phenotypes = {phenotype: mondo for phenotype, mondo in phenotypes.items() if mondo_counts[mondo] == 1}
    return phenotypes


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = f'{s3_in}/tissue-sumstats/*.out'
    outdir = f'{s3_bioindex}/diff_exp/{{}}/gtex/'

    df = spark.read.csv(srcdir, header=False, sep='\t', schema=SCHEMA) \
        .withColumn('source', lit('GTEx'))

    tissue_of_input = udf(lambda s: re.search(r'.*/([^\./]+).([^\./]+).sort.filter.out', s).group(1))
    phenotype_of_input = udf(lambda s: re.search(r'.*/([^\./]+).([^\./]+).sort.filter.out', s).group(2).lower())
    gene_to_ensemble = udf(lambda gene: re.search(f'([^\.]+)\.[^-]*-(.*)', gene).group(1))
    gene_to_name = udf(lambda gene: re.search(f'([^\.]+)\.[^-]*-(.*)', gene).group(2))

    # extract the dataset and ancestry from the filename
    df = df.withColumn('gtexPhenotype', phenotype_of_input(input_file_name())) \
        .withColumn('gtexTissue', tissue_of_input(input_file_name())) \
        .withColumn('ensemblID', gene_to_ensemble(df.gene)) \
        .withColumn('geneName', gene_to_name(df.gene))

    phenotype_map = get_phenotype_map()
    biosample_map = get_biosample_map()
    tissue_filter = udf(lambda s: s in biosample_map, BooleanType())
    phenotype_filter = udf(lambda s: s in phenotype_map, BooleanType())
    apply_tissue_map = udf(lambda s: biosample_map[s][0])
    apply_biosample_map = udf(lambda s: biosample_map[s][1])
    apply_phenotype_map = udf(lambda s: phenotype_map[s])
    df = df.filter((tissue_filter(df.gtexTissue)) & (phenotype_filter(df.gtexPhenotype))) \
        .withColumn('tissue', apply_tissue_map(df.gtexTissue)) \
        .withColumn('biosample', apply_biosample_map(df.gtexTissue)) \
        .withColumn('phenotype', apply_phenotype_map(df.gtexPhenotype)) \
        .drop('gtexPhenotype', 'gtexTissue', 'gene')

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
        .orderBy(['pValue']) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('top-tissue'))

    # done
    spark.stop()


if __name__ == '__main__':
    main()

