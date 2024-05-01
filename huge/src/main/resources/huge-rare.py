import argparse
import math
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import sqrt, exp, udf, when, lit, isnan
from pyspark.sql.types import DoubleType
from scipy.stats import norm

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


@udf(returnType=DoubleType())
def p_to_z(p_value: float) -> float:
    return float(abs(norm.ppf(p_value / 2.0)))


def calculate_bf_rare(df: DataFrame):
    df = df.withColumn('z', p_to_z(df.pValue))
    df = df.withColumn('stdErr', df.beta / df.z)
    df = df.withColumn('v', df.stdErr * df.stdErr)
    omega = 0.3696
    df = df.withColumn('bf_rare',
                       sqrt(df.v / (df.v + omega)) * exp(omega * df.beta * df.beta / (2.0 * df.v * (df.v + omega))))
    bf_rare_min = 1
    bf_rare_max = 348
    df = df.withColumn('bf_rare',
                       when((df.pValue == 0.0) | (df.z == math.inf) | (df.z == -math.inf), bf_rare_max)
                       .when(df.bf_rare < bf_rare_min, bf_rare_min)
                       .when(df.bf_rare > bf_rare_max, bf_rare_max)
                       .otherwise(df.bf_rare))
    return df


def main():

    arg_parser = argparse.ArgumentParser(prog='huge-rare.py')
    arg_parser.add_argument("--phenotype", help="Phenotype (e.g. T2D) for rare data", required=True)
    args = arg_parser.parse_args()

    genes_assoc_dir = f'{s3_in}/gene_associations/combined/{args.phenotype}'
    out_dir = f'{s3_out}/out/huge/rare/{args.phenotype}'

    spark = SparkSession.builder.appName('huge-rare').getOrCreate()

    gene_assoc = spark.read.json(f'{genes_assoc_dir}/part-*') \
        .select('gene', 'pValue', 'beta')
    gene_assoc = gene_assoc \
        .filter(gene_assoc.pValue.isNotNull() & ~isnan(gene_assoc.pValue)) \
        .filter(gene_assoc.beta.isNotNull() & ~isnan(gene_assoc.beta))

    gene_bf = calculate_bf_rare(gene_assoc) \
        .withColumn('phenotype', lit(args.phenotype))

    gene_bf.write \
        .mode('overwrite') \
        .json(out_dir)
    spark.stop()


if __name__ == '__main__':
    main()
