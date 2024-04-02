#!/usr/bin/python3
import argparse
import platform
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, concat_ws, lit, lower, regexp_replace, udf, when

S3DIR = 's3://dig-analysis-data'
# BED files need to be sorted by chrom/start, this orders the chromosomes
CHROMOSOMES = list(map(lambda c: str(c + 1), range(22))) + ['X', 'Y', 'MT']

# *-like chromatin states
ENHANCER = 'enhancer'
PROMOTER = 'promoter'
OTHER = 'other'
ACCESSIBLE_CHROMATIN = 'accessible_chromatin'

enhancer_state_prefixes = ['enh', 'h3k27ac']
promoter_state_prefixes = ['tss', 'promoter', 'h3k4me3']
other_state_prefixes = ['ctcf', 'unclass', 'transcription', 'polycomb', 'tx', 'repr', 'quies', 'het', 'znf']  # To be done later
accessible_chromatin_matches = ['dnase-only']  # To be done later


@udf(returnType=StringType())
def harmonized_state(annotation, state):
    """
    Attempts to discover the annotation used for LDSC
    given the current annotation and state columns. If the annotation
    is a chromatin state, then the state field is harmonized and
    used, otherwise the annotation is returned as-is.
    """
    annotation = annotation.lower()

    if annotation != 'candidate_regulatory_elements' and annotation != 'chromatin_state':
        return annotation

    # discover alternative states
    state = state.lower()
    if any([prefix in state for prefix in enhancer_state_prefixes]):
        return ENHANCER
    if any([prefix in state for prefix in promoter_state_prefixes]):
        return PROMOTER
    # TODO: These will be implemented later
    # if any([prefix in state for prefix in other_state_prefixes]):
    #     return OTHER
    if any([perfect_match == state for perfect_match in accessible_chromatin_matches]):
        return ACCESSIBLE_CHROMATIN
    return None


def get_optional_column(df, col_name):
    if col_name not in df.columns:
        return df.withColumn(col_name, lit('Not Available'))
    else:
        return df.withColumn(col_name, regexp_replace(df[col_name], ',', ';'))


def main():
    """
    Arguments: type/dataset, partitions
    """
    print('Python version: %s' % platform.python_version())

    opts = argparse.ArgumentParser()
    opts.add_argument('dataset')

    # extract the dataset from the command line
    args = opts.parse_args()

    partitions = ['annotation', 'tissue', 'biosample', 'dataset']

    # get the source and output directories
    srcdir = f'{S3DIR}/annotated_regions/cis-regulatory_elements/{args.dataset}/part-*'
    outdir = f'{S3DIR}/out/ldsc/regions/partitioned/{args.dataset}'

    # create a spark session
    spark = SparkSession.builder.appName('ldsc').getOrCreate()

    # read all the fields needed across the regions for the dataset
    df = spark.read.json(srcdir)

    df = get_optional_column(df, 'state')
    df = get_optional_column(df, 'biosample')
    df = get_optional_column(df, 'method')
    df = get_optional_column(df, 'source')
    df = get_optional_column(df, 'diseaseTermName')
    df = get_optional_column(df, 'dataset')

    # rename enhancer, other and promoter states, if not, make null
    df = df.withColumn('annotation', harmonized_state(df.annotation, df.state))
   
    # remove null annotations
    df = df.filter(df.annotation.isNotNull())

    # fill empty partitions
    for partition in partitions:
        df = df.fillna({partition: f'no_{partition}'})

    # fix any whitespace issues
    partition_strs = []
    for partition in partitions:
        partition_strs.append(regexp_replace(df[partition], ' ', '_'))
    # build the partition name
    partition_name = concat_ws('___', *partition_strs)

    # remove invalid chromosomes rows add a sort value and bed filename
    df = df.filter(df.chromosome.isin(CHROMOSOMES)) \
        .withColumn('partition', partition_name)
    # final output
    df = df.select(
        df.partition,
        df.chromosome,
        df.start,
        df.end,
        df.state,
        df.biosample,
        df.method,
        df.source,
        col('diseaseTermName').alias('disease'),
        df.dataset
    )

    df.orderBy(['chromosome', 'start', 'end']) \
        .coalesce(1) \
        .write \
        .mode('overwrite') \
        .partitionBy('partition') \
        .csv(outdir, sep='\t', header=False)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
