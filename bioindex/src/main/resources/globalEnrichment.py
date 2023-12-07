from pyspark.sql import SparkSession
from pyspark.sql.functions import col

s3_in = 'dig-analysis-hermes'
s3_out = 'dig-analysis-hermes/bioindex'

def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output directories
    srcdir = f's3://{s3_in}/out/ldsc/partitioned_heritability/*/*.json'
    outdir = f's3://{s3_out}/partitioned_heritability'

    # load the global enrichment summaries
    df = spark.read.json(srcdir)

    df = df.select([
        'phenotype',
        'ancestry',
        'annotation',
        'biosample',
        'tissue',
        col('SNPs').alias('expectedSNPs'),
        col('h2_beta').alias('SNPs'),
        'pValue'
    ])

    df_null = df.filter(df.biosample.isNull())
    mixed_df = df.filter(df.ancestry == 'Mixed')
    non_mixed_df = df.filter(df.ancestry != 'Mixed')

    # sort by phenotype and then p-value
    df_null.orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/annotation-tissue')

    df.orderBy(['ancestry', 'tissue', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/tissue')

    mixed_df.orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/trans-ethnic')

    non_mixed_df.orderBy(['phenotype', 'ancestry', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/ancestry')

    # done
    spark.stop()


if __name__ == '__main__':
    main()
