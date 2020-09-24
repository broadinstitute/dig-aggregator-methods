from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load and output directory
    srcdir = f's3://dig-analysis-data/variants/*/*/part-*'
    outdir = f's3://dig-bio-index/associations/variant'

    # load only the data needed across datasets for a phenotype
    df = spark.read.json(srcdir)
    df = df.select(
        df.varId,
        df.dataset,
        df.phenotype,
        df.pValue,
        df.beta,
        df.n,
    )

    # write associations sorted by phenotype, varId, then pValue
    df.orderBy(['varId']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
