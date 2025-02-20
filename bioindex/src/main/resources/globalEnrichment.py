import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # source and output directories
    srcdir = f'{s3_in}/out/ldsc/partitioned_heritability/*/*.json'
    outdir = f'{s3_bioindex}/partitioned_heritability'

    # load the global enrichment summaries
    df = spark.read.json(srcdir)

    df = df.select([
        'project',
        'phenotype',
        'ancestry',
        'annotation',
        'biosample',
        'tissue',
        col('SNPs').alias('expectedSNPs'),
        col('h2_beta').alias('SNPs'),
        col('enrichment_beta').alias('enrichment'),
        'pValue'
    ])

    df_top = df.sort(['phenotype', 'ancestry', 'annotation', 'tissue', 'pValue']) \
        .dropDuplicates(['phenotype', 'ancestry', 'annotation', 'tissue'])

    df_null = df.filter(df.biosample.isNull())
    mixed_df = df.filter(df.ancestry == 'Mixed')
    non_mixed_df = df.filter(df.ancestry != 'Mixed')

    # sort by phenotype and then p-value
    df_null.orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/annotation-tissue')

    df_top.orderBy(['ancestry', 'tissue', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/top-tissue')

    df.orderBy(['phenotype', 'ancestry', 'annotation', 'tissue', 'pValue']) \
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
