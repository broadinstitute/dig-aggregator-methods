from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output directories
    srcdir = f's3://dig-analysis-data/out/varianteffect/effects/*.json'
    outdir = f's3://dig-bio-index/transcript_consequences'

    # load the common effect data
    df = spark.read.json(srcdir)

    # join before exploding (faster) to get dbSNP
    df = df \
        .select(df.id.alias('varId'), df.transcript_consequences) \
        .withColumn('cqs', explode(df.transcript_consequences)) \
        .select(
            col('varId'),
            col('cqs.*'),
        )

    # NOTE: There's no need to order by varId because the explode took care
    #       of that implictly since each variant was only there once.

    # output the consequences, ordered by variant
    df.drop('domains') \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
