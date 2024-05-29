import os

from pyspark.sql import SparkSession

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():

    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output locations
    srcdir = f'{s3_in}/single_cell/*/dataset_metadata'
    outdir = f'{s3_bioindex}/single_cell/metadata'
    spark.read.json(srcdir) \
        .orderBy(['datasetId']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
