import argparse
import os

from pyspark.sql import SparkSession, Row

# what bucket will be output to?
OUT_BUCKET = f'dig-bio-{"test" if os.getenv("JOB_DRYRUN") else "index"}'


def main():
    """
    Arguments: phenotype
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')

    # parse cmdline
    args = opts.parse_args()

    # load and output directory
    srcdir = f's3://dig-analysis-data/variants/*/{args.phenotype}'
    outdir = f's3://{OUT_BUCKET}/datasets/variant/{args.phenotype}'

    # start the spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load all variants across all datasets for this phenotype
    df = spark.read.json(f'{srcdir}/part-*')

    # only keep the variant ID and dataset
    df = df.select(df.varId, df.dataset)

    # combine get the unique list of datasets per variant
    df = df.rdd \
        .keyBy(lambda r: r.varId) \
        .combineByKey(
            lambda r: [r.dataset],
            lambda a, b: a + [b.dataset],
            lambda a, b: a + b,
        ) \
        .map(lambda r: Row(varId=r[0], datasets=list(set(r[1])))) \
        .toDF()

    # write the set of datasets for each variant
    df.write.mode('overwrite').json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
