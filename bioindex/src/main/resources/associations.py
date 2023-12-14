import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


s3_in = 'dig-analysis-igvf'
s3_out = 'dig-analysis-igvf/bioindex'


def main():
    """
    Arguments: phenotype
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('--phenotype', type=str, required=True)
    opts.add_argument('--ancestry', type=str, required=True)

    # parse command line
    args = opts.parse_args()

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load the trans-ethnic, meta-analysis, top variants and write them sorted
    if args.ancestry == 'Mixed':
        srcdir = f's3://{s3_in}/out/metaanalysis/bottom-line/trans-ethnic/{args.phenotype}/part-*'
        outdir = f's3://{s3_out}/associations/phenotype/trans-ethnic/{args.phenotype}'
    else:
        srcdir = f's3://{s3_in}/out/metaanalysis/bottom-line/ancestry-specific/{args.phenotype}/ancestry={args.ancestry}/part-*'
        outdir = f's3://{s3_out}/ancestry-associations/phenotype/{args.phenotype}/{args.ancestry}'

    df = spark.read.json(srcdir) \
        .withColumn('ancestry', lit(args.ancestry))

    # common vep data (can we cache this?)
    common_dir = f's3://{s3_in}/out/varianteffect/common/part-*'
    common = spark.read.json(common_dir)

    # join the common data
    df = df.join(common, 'varId', how='left_outer')

    # write associations sorted by locus and then variant id
    df.orderBy(['chromosome', 'position', 'varId']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
