from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output directories
    srcdir = f's3://dig-analysis-data/out/ldsc/partitioned_heritability/*/*.json'
    outdir = f's3://dig-bio-index/partitioned_heritability'

    # load the global enrichment summaries
    df = spark.read.json(srcdir)

    df = df.select([
        'phenotype',
        'ancestry',
        'annotation',
        'tissue',
        col('SNPs').alias('expectedSNPs'),
        col('h2_beta').alias('SNPs'),
        'pValue'
    ])

    # sort by phenotype and then p-value
    df.orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
