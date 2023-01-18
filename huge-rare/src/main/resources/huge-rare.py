import argparse
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from pyspark.sql.functions import sqrt, exp, udf
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
    return df


def main():
    """
    Arguments: none
    """
    print('Hello! The time is now ', str(datetime.now()))
    print('Now building argument parser')
    arg_parser = argparse.ArgumentParser(prog='huge-rare.py')
    arg_parser.add_argument("--phenotype", help="The phenotype", required=True)
    arg_parser.add_argument("--gene-associations", help="Gene data with p-values", required=True)
    arg_parser.add_argument("--out-dir", help="Output directory", required=True)
    print('Now parsing CLI arguments')
    cli_args = arg_parser.parse_args()
    phenotype = cli_args.phenotype
    files_glob = 'part-*'
    genes_assoc_glob = cli_args.gene_associations + files_glob
    out_dir = cli_args.out_dir
    print('Phenotype: ', phenotype)
    print('Gene data with p-values: ', genes_assoc_glob)
    print('Output directory: ', out_dir)
    spark = SparkSession.builder.appName('huge-rare').getOrCreate()
    # spark = SparkSession.builder.appName('huge-rare') \
    #     .config('spark.driver.memory', '6g').config('spark.driver.maxResultSize', '2g').getOrCreate()
    gene_assoc = spark.read.json(genes_assoc_glob).select('gene', 'pValue', 'beta')
    gene_bf = calculate_bf_rare(gene_assoc)
    print('Now writing to ', out_dir)
    gene_bf.write.mode('overwrite').json(out_dir)
    print('Done with work, stopping Spark')
    spark.stop()
    print('Spark stopped')


if __name__ == '__main__':
    main()
