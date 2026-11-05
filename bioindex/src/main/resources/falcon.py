import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']

def main():
    srcdir = f'{s3_in}/out/falcon/genes/*/falcon.genes'
    outdir = f'{s3_bioindex}/falcon/genes/{{}}'

    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    df = spark.read.json(srcdir)

    df.orderBy(["GENE", col("PROBABILITY").desc()]) \
        .write \
        .mode('overwrite') \
        .json(outdir.format("genes"))

    df.orderBy(["TRAIT", col("PROBABILITY").desc()]) \
        .write \
        .mode('overwrite') \
        .json(outdir.format("trait"))

    spark.stop()


if __name__ == '__main__':
    main()
