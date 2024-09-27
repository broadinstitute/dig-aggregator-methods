import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']

outdir = f'{s3_bioindex}/pigean/{{}}/{{}}'


def get_phenotype_map():
    phenotype_map = {}
    with open('/mnt/var/pigean/study_to_efo.tsv', 'r') as f:
        for line in f:
            split_line = line.strip().split('\t')
            if len(split_line) == 2:
                phenotype_map[split_line[1]] = split_line[0]
    return phenotype_map


def attach_max_values(df, fields):
    max_values = df \
        .select('phenotype', *fields) \
        .groupBy(['phenotype']) \
        .agg({field: 'max' for field in fields}) \
        .select('phenotype', *[col(f'max({field})').alias(f'max_trait_{field}') for field in fields])  # rename
    return df.join(max_values, how='left', on='phenotype')


def bioindex(spark, srcdir, bioindex_name, bioindices, max_fields, phenotype_map):
    study_to_phenotype = udf(lambda study: phenotype_map[study])
    df = spark.read.json(srcdir)
    df = df.withColumn('phenotype', study_to_phenotype(df.phenotype))
    for name, order in bioindices.items():
        if len(max_fields) > 0 and name != 'phenotype':
            df_out = attach_max_values(df, max_fields)
        else:
            df_out = df
        df_out.orderBy(order) \
            .write \
            .mode('overwrite') \
            .json(outdir.format(bioindex_name, name))


def gene(spark, phenotype_map):
    srcdir = f'{s3_in}/out/pigean/gene_stats/*/*.json'
    bioindices = {
        'gene': [col('gene'), col('combined').desc()],
        'phenotype': [col('phenotype'), col('combined').desc()]
    }
    bioindex(spark, srcdir, 'gene', bioindices, ['prior', 'combined', 'log_bf'], phenotype_map)


def gene_set(spark, phenotype_map):
    srcdir = f'{s3_in}/out/pigean/gene_set_stats/*/*.json'
    bioindices = {
        'gene_set': [col('gene_set'), col('beta').desc()],
        'phenotype': [col('phenotype'), col('beta').desc()]
    }
    bioindex(spark, srcdir, 'gene_set', bioindices, ['beta', 'beta_uncorrected'], phenotype_map)


def gene_gene_set(spark, phenotype_map):
    srcdir = f'{s3_in}/out/pigean/gene_gene_set_stats/*/*.json'
    bioindices = {
        'gene': [col('phenotype'), col('gene'), col('combined').desc()],
        'gene_set': [col('phenotype'), col('gene_set'), col('beta').desc()]
    }
    bioindex(spark, srcdir, 'gene_gene_set', bioindices, [], phenotype_map)


def main():
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    phenotype_map = get_phenotype_map()
    gene(spark, phenotype_map)
    gene_set(spark, phenotype_map)
    gene_gene_set(spark, phenotype_map)

    spark.stop()


if __name__ == '__main__':
    main()
