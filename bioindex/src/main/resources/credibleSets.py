from pyspark.sql import SparkSession

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def write(df, name):
    df.orderBy(['credibleSetId', 'chromosome', 'position']) \
        .write \
        .mode('overwrite') \
        .json(f'{s3_bioindex}/credible_sets/{name}/variants/')

    df.withColumnRenamed('clumpStart', 'start') \
        .withColumnRenamed('clumpEnd', 'end') \
        .select('phenotype', 'dataset', 'method', 'pmid', 'credibleSetId', 'chromosome', 'start', 'end') \
        .orderBy(['chromosome', 'start']) \
        .mode('overwrite') \
        .json(f'{s3_bioindex}/credible_sets/{name}/locus/')


def main():
    # read all
    srcdir = f'{s3_in}/out/credible_sets/merged/*/*/part-*'

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load all the credible sets for the phenotype
    df = spark.read.json(srcdir)
    write(df[df.ancestry == 'Mixed'], 'trans-ethnic')
    write(df[df.ancestry != 'Mixed'], 'ancestry-specific')

    # done
    spark.stop()


if __name__ == '__main__':
    main()
