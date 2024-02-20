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
         'beta', 'stdErr', 'pValue',
         'phenotype', 'ancestry', 'gwas_dataset', 'credibleSetId', 'posteriorProbability']
    ).withColumnRenamed('gwas_dataset', 'dataset')\
        .dropDuplicates(['credibleSetId', 'varId'])
    df = df.withColumn('source', lit('credible_set'))

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
def credible_set_id_from_clump(clump):
    return f'bottom_line_clump_{clump}'


@udf(returnType=DoubleType())
def p_to_z(p_value):
    return float(abs(norm.ppf(p_value / 2.0)))


# define posteriorProbability as Bayes Factor
def bayes_pp(df):
    # https://pubmed.ncbi.nlm.nih.gov/18642345/ - An implicit p-value prior
    # k set s.t. posterior probability of ~0.75 for p=5e-8
    k = 0.974
    df = df.withColumn('z', p_to_z(df.pValue))
    df = df.withColumn('abf', 1 / (1 + k)**0.5 * exp(df.z * df.z * k / 2 / (1 + k)))
    bayes_sum = df.groupBy('clump')\
        .agg(sum('abf').alias('abfSum'))
    df = df.join(bayes_sum, on=['clump'], how='left')
    return df.withColumn('posteriorProbability', df.abf / df.abfSum)\
        .drop('z', 'abf', 'abfSum')


def convert_clump_file(phenotype, ancestry, df):
    df = df.select(
        ['varId', 'chromosome', 'position', 'reference', 'alt',
         'beta', 'stdErr', 'pValue',
         'phenotype', 'clump', 'clumpStart', 'clumpEnd', 'leadSNP', 'alignment']
    ) \
        .withColumn('ancestry', lit(ancestry)) \
        .withColumn('credibleSetId', credible_set_id_from_clump(df.clump)) \
        .withColumn('dataset', lit(f'BL_{phenotype}_{ancestry}')) \
        .withColumn('source', lit('bottom_line')) \
        .drop('clump')

    df = bayes_pp(df)

    return df


def get_out_dir(df):
    df_first = df.first()
    ancestry = df_first['ancestry']
    source = df_first['source']
    dataset = df_first['dataset']
    phenotype = df_first['phenotype']
    return f'{s3_out}/out/credible_sets/{phenotype}/{ancestry}/{source}/{dataset}/'


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
        df = convert_clump_file(args.phenotype, args.ancestry, df)
    out_dir = get_out_dir(df)

    save_df(df, out_dir)
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
