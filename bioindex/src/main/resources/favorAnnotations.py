import os
from pyspark.sql import SparkSession

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = f'{s3_in}/annotated_regions/FAVOR_annot/basic_info'
    outdir = f'{s3_bioindex}/FAVOR_annot/basic_info'

    df = spark.read.csv(f'{srcdir}/*.tsv.gz')

    # sort and write
    df.orderBy(['varId']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
