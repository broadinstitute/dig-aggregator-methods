#!/usr/bin/python3
import argparse
import platform

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import concat_ws, lower, regexp_replace, udf, when

# S3DIR = 's3://dig-analysis-data'
S3DIR = 's3://cmdga-analysis-data'

# BED files need to be sorted by chrom/start, this orders the chromosomes
CHROMOSOMES = list(map(lambda c: str(c + 1), range(22))) + ['X', 'Y', 'MT']

# *-like chromatin states
ENHANCER = 'enhancer'
PROMOTER = 'promoter'

# mapping of harmonized chromatin states
CHROMATIN_STATES = {
    'enhancer': ENHANCER,
    'genic_enhancer': ENHANCER,
    'enhancer_genic': ENHANCER,
    'enhancer_genic_1': ENHANCER,
    'enhancer_genic_2': ENHANCER,
    'active_enhancer_1': ENHANCER,
    'enhancer_active_1': ENHANCER,
    'active_enhancer_2': ENHANCER,
    'enhancer_active_2': ENHANCER,
    'weak_enhancer': ENHANCER,
    'enhancer_weak': ENHANCER,
    'enhancer_bivalent': ENHANCER,
    'enh': ENHANCER,
    'enhg': ENHANCER,
    'enhg1': ENHANCER,
    'enhg2': ENHANCER,
    'enha1': ENHANCER,
    'enha2': ENHANCER,
    'enhwk': ENHANCER,
    'enhbiv': ENHANCER,

    # promoter-like states
    'promoter': PROMOTER,
    'promoter_weak': PROMOTER,
    'promoter_flanking': PROMOTER,
    'promoter_active': PROMOTER,
    'promoter_bivalent': PROMOTER,
    'promoter_bivalent_flanking': PROMOTER,
    'promoter_flanking_upstream': PROMOTER,
    'promoter_flanking_downstream': PROMOTER,
    'weak_tss': PROMOTER,
    'flanking_tss': PROMOTER,
    'active_tss': PROMOTER,
    'bivalent_tss': PROMOTER,
    'poised_tss': PROMOTER,
    'bivalent/poised_tss': PROMOTER,
    'tssaflnk': PROMOTER,
    'tssflnk': PROMOTER,
    'tssa': PROMOTER,
    'tssbiv': PROMOTER,
    'bivflnk': PROMOTER,
    'tssflnku': PROMOTER,
    'tssflnkd': PROMOTER,
}


@udf(returnType=StringType())
def harmonized_state(annotation, state):
    """
    Attempts to discover the annotation used for LDSC and GREGOR
    given the current annotation and state columns. If the annotation
    is a chromatin state, then the state field is harmonized and
    used, otherwise the annotation is returned as-is.
    """
    annotation = annotation.lower()
    state = state.lower()

    if annotation != 'chromatin_state':
        return annotation

    # discover enhancer and promoter-like states
    if 'enh' in state:
        return ENHANCER
    if 'tss' in state or 'promoter' in state:
        return PROMOTER

    return None


def main():
    """
    Arguments: type/dataset
    """
    print('Python version: %s' % platform.python_version())

    opts = argparse.ArgumentParser()
    opts.add_argument('dataset')

    # extract the dataset from the command line
    args = opts.parse_args()

    # get the source and output directories
    #srcdir = f'{S3DIR}/annotated_regions/cis-regulatory_elements/{args.dataset}/part-*'
    srcdir = f'{S3DIR}/cis-regulatory_elements/{args.dataset}/part-*'
    outdir = f'{S3DIR}/out/ldsc/regions/partitioned/{args.dataset}'

    # create a spark session
    spark = SparkSession.builder.appName('ldsc').getOrCreate()

    # read all the fields needed across the regions for the dataset
    df = spark.read.json(srcdir)

    # rename enhancer and promoter states, if not, make null
    df = df.withColumn('annotation', harmonized_state(df.annotation, df.state))

    # remove null annotations
    df = df.filter(df.annotation.isNotNull())

    # fix any whitespace issues
    annotation = regexp_replace(df.annotation, ' ', '_')
    tissue = regexp_replace(df.tissue, ' ', '_')
    dataset = regexp_replace(df.dataset, ' ', '_')
    biosample = regexp_replace(df.biosample, ' ', '_')
    method = regexp_replace(df.method, ' ', '_')

    # build the partition name
    partition = concat_ws('___', dataset, annotation, tissue, biosample, source, method)

    # remove invalid chromosomes rows add a sort value and bed filename
    df = df.filter(df.chromosome.isin(CHROMOSOMES)) \
        .withColumn('partition', partition)

    # final output
    df = df.select(
        df.partition,
        df.chromosome,
        df.start,
        df.end,
    )

    # output the regions partitioned for GREGOR in BED format
    df.write \
        .mode('overwrite') \
        .partitionBy('partition') \
        .csv(outdir, sep='\t', header=False)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
