import argparse
import math
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import sqrt, exp, udf, when
from pyspark.sql.types import DoubleType
from scipy.stats import norm


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
    """
    Arguments: none
    """
    arg_parser = argparse.ArgumentParser(prog='huge-rare.py')
    arg_parser.add_argument("--gene-associations", help="Gene data with p-values", required=True)
    arg_parser.add_argument("--out-dir", help="Output directory", required=True)
    cli_args = arg_parser.parse_args()
    files_glob = 'part-*'
    genes_assoc_glob = cli_args.gene_associations + files_glob
    out_dir = cli_args.out_dir
    spark = SparkSession.builder.appName('huge-rare').getOrCreate()
    gene_assoc = spark.read.json(genes_assoc_glob).select('gene', 'pValue', 'beta')
    gene_bf = calculate_bf_rare(gene_assoc)
    gene_bf.write.mode('overwrite').json(out_dir)
    spark.stop()


if __name__ == '__main__':
    main()
