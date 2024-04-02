#!/usr/bin/python3
from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, exp, lit, min, max, signum, sum, udf, when
from pyspark.sql.types import StringType, DoubleType

import numpy as np
from scipy.stats import norm

s3_in = 's3://dig-analysis-data'
s3_out = 's3://dig-analysis-data'


def get_src_dir(args):
    if args.source == 'credible-set':
        src_dir = f'{s3_in}/credible_sets/{args.dataset}/{args.phenotype}/part-*'
    else:
        if args.ancestry != 'Mixed':
            src_dir = f'{s3_in}/out/metaanalysis/bottom-line/ancestry-clumped/{args.phenotype}/ancestry={args.ancestry}/part-*'
        else:
            src_dir = f'{s3_in}/out/metaanalysis/bottom-line/clumped/{args.phenotype}/part-*'
    return src_dir


def convert_credible_set(df):
    # Define source and fetch proper columns
    if 'gwas_dataset' not in df.columns:
        df = df.withColumn('gwas_dataset', df.dataset)
    df = df.select(
        ['varId', 'chromosome', 'position', 'reference', 'alt',
         'beta', 'stdErr', 'pValue', 'n',
         'phenotype', 'ancestry', 'gwas_dataset', 'dataset', 'credibleSetId', 'posteriorProbability']
    ).filter(df.varId.isNotNull()) \
        .dropDuplicates(['credibleSetId', 'varId'])
    df = df.withColumn('dataset', when(df.gwas_dataset.isNull(), df.dataset).otherwise(df.gwas_dataset)) \
        .drop('gwas_dataset') \
        .withColumn('source', lit('credible_set')) \
        .withColumn('chromosome', df.chromosome.cast("string"))

    # TODO: This shouldn't be necessary, but we need external credible sets and out credible sets on an equal footing
    # Normalize
    pp_sum = df.groupBy(['credibleSetId'])\
        .agg(sum('posteriorProbability').alias('ppSum'))
    df = df.join(pp_sum, on='credibleSetId', how='left')
    df = df.withColumn('posteriorProbability', df.posteriorProbability / df.ppSum)\
        .drop('ppSum')

    # Attach start/end to credible sets
    ranges = df.groupBy('credibleSetId').agg(
        min('position').alias('clumpStart'),
        max(df.position + 1).alias('clumpEnd')
    )

    max_posterior = df.sort(['credibleSetId', 'posteriorProbability'], ascending=False)
    lead_snps = max_posterior.dropDuplicates(['credibleSetId']) \
        .select(col('credibleSetId'), col('varId'), lit(True).alias('leadSNP')) \
        .drop('posteriorProbability')

    df = df.join(ranges, on=['credibleSetId'], how='left')
    df = df.join(lead_snps, on=['credibleSetId', 'varId'], how='left')
    df = df.na.fill({'leadSNP': False})

    # Generate alignment
    lead_snps = df.filter(df.leadSNP == True) \
        .select(df.credibleSetId, df.beta.alias('alignment'))
    df = df.join(lead_snps, on='credibleSetId')
    df = df.withColumn('alignment', signum(df.beta * df.alignment))

    df = df.withColumn('pValue', when(df.pValue == 0.0, np.nextafter(0, 1)).otherwise(df.pValue))

    return df


@udf(returnType=StringType())
def credible_set_id_from_clump(clump, meta_type, param_type, freq_type):
    return f'{clump}_{meta_type}_{param_type}_{freq_type}'


@udf(returnType=StringType())
def get_source(meta_type, param_type, freq_type):
    return f'{meta_type}_{param_type}_{freq_type}'


@udf(returnType=StringType())
def get_dataset(meta_type, param_type, phenotype, ancestry):
    return f'{meta_type}_{param_type}_{phenotype}_{ancestry}'


@udf(returnType=DoubleType())
def p_to_z(p_value):
    # Becomes infinity for lower values, smallest possible value marginally less at 5E-324
    if p_value < 1E-323:
        return float(abs(norm.ppf(1E-323 / 2.0)))
    else:
        return float(abs(norm.ppf(p_value / 2.0)))


# define posteriorProbability as Bayes Factor
def bayes_pp(df):
    # https://pubmed.ncbi.nlm.nih.gov/18642345/ - An implicit p-value prior
    # k set s.t. posterior probability of ~0.75 for p=5e-8
    k = 0.974
    df = df.withColumn('z', p_to_z(df.pValue))
    df = df.withColumn('abf', 1 / (1 + k)**0.5 * exp(df.z * df.z * k / 2 / (1 + k)))
    bayes_sum = df.groupBy('credibleSetId')\
        .agg(sum('abf').alias('abfSum'))
    df = df.join(bayes_sum, on=['credibleSetId'], how='left')
    return df.withColumn('posteriorProbability', df.abf / df.abfSum)\
        .drop('z', 'abf', 'abfSum')


def convert_clump_file(ancestry, df):
    df = df.select(
        ['varId', 'chromosome', 'position', 'reference', 'alt',
         'metaType', 'paramType', 'freqType',
         'beta', 'stdErr', 'pValue', 'n',
         'phenotype', 'clump', 'clumpStart', 'clumpEnd', 'leadSNP', 'alignment']
    ) \
        .withColumn('ancestry', lit(ancestry))
    df = df \
        .withColumn('credibleSetId', credible_set_id_from_clump(df.clump, df.metaType, df.paramType, df.freqType)) \
        .withColumn('dataset', get_dataset(df.metaType, df.paramType, df.phenotype, df.ancestry)) \
        .withColumn('source', get_source(df.metaType, df.paramType, df.freqType)) \
        .drop('clump', 'metaType', 'paramType', 'freqType')

    df = bayes_pp(df)

    return df


def get_out_dir(df, meta_param):
    df_first = df.first()
    ancestry = df_first['ancestry']
    dataset = df_first['dataset']
    phenotype = df_first['phenotype']
    return f'{s3_out}/out/credible_sets/intake/{phenotype}/{ancestry}/{meta_param}/{dataset}/'


def save_df(df, out_dir):
    df.write \
        .mode('overwrite') \
        .json(out_dir)


def main():
    opts = ArgumentParser()
    opts.add_argument('--source', type=str, required=True)
    opts.add_argument('--dataset', type=str)
    opts.add_argument('--phenotype', type=str, required=True)
    opts.add_argument('--ancestry', type=str)
    args = opts.parse_args()

    src_dir = get_src_dir(args)

    spark = SparkSession.builder.appName('credible-sets').getOrCreate()

    df = spark.read.json(src_dir)
    if args.source == 'credible-set':
        df = convert_credible_set(df)
    else:
        df = convert_clump_file(args.ancestry, df)
    # TODO: To use alternative meta/param types, pass through as arguments
    meta_param = 'credible_set' if args.source == 'credible-set' else 'bottom-line_portal'
    out_dir = get_out_dir(df, meta_param)

    save_df(df, out_dir)
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
