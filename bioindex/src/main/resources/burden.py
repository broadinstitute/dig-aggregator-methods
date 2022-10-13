import argparse
from pyspark.sql import SparkSession


key_map = {
    'gene': 'geneSymbol',
    'transcript': 'transcriptId'
}


def main():
    """
    Arguments: none
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('run_type')
    args = opts.parse_args()

    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load and output directory
    srcdir = f's3://dig-analysis-data/out/burdenbinning/{args.run_type}/*'
    outdir = f's3://dig-bio-index/burden'
    key = key_map[args.run_type]

    df = spark.read.json(f'{srcdir}/part-*')

    # sort by gene and then bin
    df.orderBy([key, 'burdenBinId']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/{args.run_type}')

    # sort by datatype, gene and then bin
    df.orderBy(['datatype', key, 'burdenBinId']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/datatype/{args.run_type}')

    # done
    spark.stop()


if __name__ == '__main__':
    main()
