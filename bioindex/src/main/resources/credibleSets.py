import os
from pyspark.sql import SparkSession

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def write(df, name):
    df.orderBy(['phenotype', 'ancestry', 'credibleSetId', 'chromosome', 'position']) \
        .write \
        .mode('overwrite') \
        .json(f'{s3_bioindex}/credible_sets/variants/{name}/')

    df.withColumnRenamed('clumpStart', 'start') \
        .withColumnRenamed('clumpEnd', 'end') \
        .select('phenotype', 'ancestry', 'dataset', 'method', 'pmid', 'credibleSetId', 'chromosome', 'start', 'end') \
        .drop_duplicates() \
        .orderBy(['phenotype', 'ancestry', 'chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(f'{s3_bioindex}/credible_sets/locus/{name}/')


def main():
    # read all
    srcdir = f'{s3_in}/out/credible_sets/merged/*/*/part-*'

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load all the credible sets for the phenotype
    df = spark.read.json(srcdir)
    write(df[df.ancestry == 'Mixed'], 'trans-ethnic')
    write(df[df.ancestry != 'Mixed'], 'ancestry')

    # done
    spark.stop()


if __name__ == '__main__':
    main()
