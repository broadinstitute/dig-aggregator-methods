import argparse
import os

from pyspark.sql import SparkSession

# what bucket will be output to?
S3_BUCKET = os.getenv('JOB_BUCKET')


def main():
    """
    Arguments: [-n kb] phenotype
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('-n', type=int, default=50)
    opts.add_argument('phenotype')

    # parse command line
    args = opts.parse_args()

    # n is in kb
    args.n *= 1000

    # source data and output location
    srcdir = f'{S3_BUCKET}/out/metaanalysis/trans-ethnic/{args.phenotype}/part-*'
    outdir = f'{S3_BUCKET}/out/metaanalysis/top/{args.phenotype}'

    # initialize spark session
    spark = SparkSession.builder.appName('bottom-line').getOrCreate()

    # load the trans-ethnic, meta-analysis
    df = spark.read.json(srcdir)

    # pick the top variant every args.n kb across the genome per phenotype
    df = df.rdd \
        .keyBy(lambda r: (r.chromosome, r.position // args.n)) \
        .reduceByKey(lambda a, b: b if b.pValue < a.pValue else a) \
        .map(lambda r: r[1]) \
        .toDF()

    # write out just the top associations, indexed by locus
    df.write.mode('overwrite').json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
