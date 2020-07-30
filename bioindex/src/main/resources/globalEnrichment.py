from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output directories
    srcdir = f's3://dig-analysis-data/out/gregor/enrichment/*/part-*'
    outdir = f's3://dig-bio-index/global_enrichment'

    # tissue ontology
    tissue_dir = 's3://dig-analysis-data/tissues/ontology/part-*'

    # load the global enrichment and join with tissue ontology
    df = spark.read.json(srcdir)
    tissues = spark.read.json(tissue_dir)

    # join with tissues, sort, and write
    df.join(tissues, 'tissueId', how='left_outer') \
        .orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/phenotype')

    # done
    spark.stop()


if __name__ == '__main__':
    main()
