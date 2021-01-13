from argparse import ArgumentParser
from pyspark.sql import SparkSession, Row


def main():
    """
    Arguments: ancestry
    """
    opts = ArgumentParser()
    opts.add_argument('ancestry')

    # args.phenotype will be set
    args = opts.parse_args()

    srcdir = f's3://dig-analysis-data/variants/*/*'
    outdir = f's3://dig-analysis-data/out/frequencyanalysis/{args.ancestry}'

    # create a spark session
    spark = SparkSession.builder.appName('frequencyanalysis').getOrCreate()

    # load all the variants across all datasets
    df = spark.read.json(f'{srcdir}/part-*')

    # keep only the requested ancestry for variants that have frequency data
    df = df.filter(df.maf.isNotNull() & (df.ancestry == args.ancestry))

    # only need varId and MAF
    df = df.select(df.varId, df.maf)

    # calculate the max MAF per variant across all datasets
    df = df.rdd \
        .reduceByKey(max) \
        .map(lambda r: Row(varId=r[0], ancestry=args.ancestry, maf=r[1])) \
        .toDF()

    # output to S3
    df.write.mode('overwrite').json(outdir)

    # done
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
