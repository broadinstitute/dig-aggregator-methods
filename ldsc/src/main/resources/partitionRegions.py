#!/usr/bin/python3
import argparse
import platform
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import concat_ws, lower, regexp_replace, udf, when
from pyspark.sql.functions import lit

S3DIR = 's3://dig-analysis-data'
# BED files need to be sorted by chrom/start, this orders the chromosomes
CHROMOSOMES = list(map(lambda c: str(c + 1), range(22))) + ['X', 'Y', 'MT']

# *-like chromatin states
ENHANCER = 'enhancer'
PROMOTER = 'promoter'
OTHER = 'other'
ACCESSIBLE_CHROMATIN = 'accessible_chromatin'

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
    'Distal_enhancer-like': ENHANCER,
    'High-H3K27ac': ENHANCER,
    'Proximal_enhancer-like': ENHANCER,
    
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
    'DNase-H3K4me3': PROMOTER,
    'High-H3K4me3': PROMOTER,
    'Promoter-like': PROMOTER,

    # other states
    'CTCF-bound': OTHER,
    'CTCF-only': OTHER,
    'High-CTCF': OTHER,
    'Unclassified': OTHER,
    'Strong_transcription': OTHER,
    'Repressed_polycomb': OTHER,
    'Weak_repressed_polycomb': OTHER,
    'Quiescent/low_signal': OTHER,
    'Weak_transcription': OTHER,
    'Tx': OTHER,
    'Txn': OTHER,
    'ReprPC': OTHER,
    'ReprPCWk': OTHER,
    'Quies': OTHER,
    'TxWk': OTHER,
    'Het': OTHER,
    'ZNF/Rpts': OTHER,
    'TxFlnk': OTHER,
    'Ctcf': OTHER,

    #accessible chromatin
    'DNase-only': ACCESSIBLE_CHROMATIN,
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

    if annotation != 'candidate_regulatory_elements' and annotation != 'chromatin_state':
        return annotation
    # discover enhancer and promoter-like states
    state = state.lower()
    if 'enh' in state or 'h3k27ac' in state:
        return ENHANCER
    if 'tss' in state or 'promoter' in state or 'h3k4me3' in state or 'h3k4me3' in state:
        return PROMOTER
    if 'ctcf' in state or 'unclass' in state or 'transcription' in state or 'polycomb' in state or 'tx' in state or 'repr' in state or 'quies' in state or 'het' in state or 'znf' in state:
        return OTHER
    if re.match('dnase-only', state):
        return ACCESSIBLE_CHROMATIN
    return None


def main():
    """
    Arguments: type/dataset, partitions
    """
    print('Python version: %s' % platform.python_version())

    opts = argparse.ArgumentParser()
    opts.add_argument('dataset')
    opts.add_argument('sub_region')

    # extract the dataset from the command line
    args = opts.parse_args()

    partitions = args.sub_region.split('-') if args.sub_region != "default" else []

    # get the source and output directories
    srcdir = f'{S3DIR}/annotated_regions/cis-regulatory_elements/{args.dataset}/part-*'
    outdir = f'{S3DIR}/out/ldsc/regions/{args.sub_region}/partitioned/{args.dataset}'

    # create a spark session
    spark = SparkSession.builder.appName('ldsc').getOrCreate()

    # read all the fields needed across the regions for the dataset
    df = spark.read.json(srcdir)
    if 'state' not in df.columns:
        df = df.withColumn('state',lit('Not Available')) 
    # rename enhancer, other and promoter states, if not, make null
    df = df.withColumn('annotation', harmonized_state(df.annotation, df.state))
   
    # remove null annotations
    df = df.filter(df.annotation.isNotNull())

    # fill empty partitions
    df = df.fillna({'tissue': 'no_tissue'})
    for partition in partitions:
        df = df.fillna({partition: f'no_{partition}'})

    # fix any whitespace issues
    partition_strs = [regexp_replace(df.annotation, ' ', '_'), regexp_replace(df.tissue, ' ', '_')]
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
