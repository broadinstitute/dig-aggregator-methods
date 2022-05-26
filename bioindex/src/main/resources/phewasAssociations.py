from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load and output directory
    srcdir = f's3://dig-analysis-data/out/metaanalysis/trans-ethnic'
    clumpdir = f's3://dig-analysis-data/out/metaanalysis/clumped'
    outdir = f's3://dig-bio-index/associations/phewas'

    # load all trans-ethnic, meta-analysis results for all variants
    df = spark.read.json(f'{srcdir}/*/part-*')
    clump_df = spark.read.json(f'{clumpdir}/*/part-*')

    # limit the data being written
    df = df.select(
        df.varId,
        df.chromosome,
        df.position,
        df.phenotype,
        df.pValue,
        df.beta,
        df.stdErr,
        df.n,
    )

    clump_df = clump_df.select(
        clump_df.varId,
        clump_df.phenotype,
        clump_df.clump
    )

    df = df.join(clump_df, on=['varId', 'phenotype'], how='left')

    # write associations sorted by variant and then p-value
    df.orderBy(['chromosome', 'position', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
