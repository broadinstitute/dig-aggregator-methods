#!/usr/bin/python3

import argparse
import platform
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType, DoubleType

s3dir = 's3://dig-analysis-igvf'

variants_schema = StructType([
    StructField('varId', StringType(), nullable=False),
    StructField('chromosome', StringType(), nullable=False),
    StructField('position', IntegerType(), nullable=False),
    StructField('reference', StringType(), nullable=False),
    StructField('alt', StringType(), nullable=False),
    StructField('multiAllelic', BooleanType(), nullable=False),
    StructField('dataset', StringType(), nullable=False),
    StructField('phenotype', StringType(), nullable=False),
    StructField('ancestry', StringType(), nullable=True),
    StructField('pValue', DoubleType(), nullable=False),
    StructField('beta', FloatType(), nullable=True),
    StructField('oddsRatio', DoubleType(), nullable=True),
    StructField('eaf', FloatType(), nullable=True),
    StructField('maf', FloatType(), nullable=True),
    StructField('stdErr', FloatType(), nullable=True),
    StructField('zScore', FloatType(), nullable=True),
    StructField('n', FloatType(), nullable=True),
])


# Because the input is in one file repartition. 100 chosen arbitrarily, downstream tasks repartitions automatically
def read_variants_json(spark, srcdir):
    return spark.read.json(srcdir, schema=variants_schema).repartition(100)


def write_variant_json(df, outdir):
    df.write\
        .mode('overwrite') \
        .option("ignoreNullFields", "false") \
        .option("compression", "org.apache.hadoop.io.compress.ZStandardCodec") \
        .json(outdir)

def copy_metadata(srcdir, outdir):
    subprocess.check_call(['aws', 's3', 'cp', f'{srcdir}/metadata', f'{outdir}/metadata'])


class VariantColumnFilter:
    def __init__(self, column_name, regex_pattern, nullable=True, value_range=None):
        self.column_name = column_name
        self.regex_pattern = regex_pattern
        self.nullable = nullable
        self.value_range = value_range

    def apply_value_range(self, spark_df):
        if self.value_range is None:
            return spark_df[self.column_name].rlike(self.regex_pattern)
        else:
            return spark_df[self.column_name].rlike(self.regex_pattern) & \
                   (spark_df[self.column_name].cast('float') >= self.value_range[0]) & \
                   (spark_df[self.column_name].cast('float') <= self.value_range[1])

    def apply_nullable(self, spark_df):
        if self.nullable:
            return (self.apply_value_range(spark_df)) | \
                   spark_df[self.column_name].isNull()
        else:
            return (self.apply_value_range(spark_df)) & \
                   ~spark_df[self.column_name].isNull()

    def split(self, spark_df):
        condition = self.apply_nullable(spark_df)
        return spark_df.filter(~condition), spark_df.filter(condition)


good_chromosome = "^([1-9]{1}|1[0-9]{1}|2[0-4]{1}|X|Y|XY|MT)$"  # 1-24 + X + Y + XY + MT are valid
good_positive_integer = "^([1-9]{1}[0-9]*|0)$"  # positive or zero only
good_float = "^-?[0-9]+.?[0-9]*[eE]?-?[0-9]*$"  # includes scientific notation and signed
good_positive_float = "^[0-9]+.?[0-9]*[eE]?-?[0-9]*$"  # includes scientific notation
good_base = "^[atcgATCG]+$"  # case insensitive, only ATCG, no multialleles (commas)

filters_to_run = [
    VariantColumnFilter("chromosome", good_chromosome, nullable=False),
    VariantColumnFilter("position", good_positive_integer, nullable=False),
    VariantColumnFilter("reference", good_base, nullable=False),
    VariantColumnFilter("alt", good_base, nullable=False),
    VariantColumnFilter("pValue", good_positive_float, nullable=False, value_range=[0, 1]),
    VariantColumnFilter("oddsRatio", good_positive_float),
    VariantColumnFilter("beta", good_float),
    VariantColumnFilter("stdErr", good_positive_float),
    VariantColumnFilter("eaf", good_positive_float, value_range=[0, 1]),
    VariantColumnFilter("n", good_positive_float)
]

# entry point
if __name__ == '__main__':
    """
    @param method/dataset/phenotype e.g. `GWAS/Anstee2020_NAFLD_eu/NAFLD`
    """
    print('Python version: %s' % platform.python_version())

    # get method/dataset/phenotype from commandline
    opts = argparse.ArgumentParser()
    opts.add_argument('method_dataset_phenotype')
    args = opts.parse_args()

    tech, dataset, phenotype = args.method_dataset_phenotype.split('/')

    # get the source and output directories (method_dataset is formatted as method/dataset here)
    srcdir = f'{s3dir}/variants_processed/{args.method_dataset_phenotype}'
    outdir = f'{s3dir}/variants_qc/{args.method_dataset_phenotype}/pass'
    qcdir = f'{s3dir}/variants_qc/{args.method_dataset_phenotype}/fail'

    # create a spark session and dataframe from part files
    spark = SparkSession.builder.appName('qc').getOrCreate()
    df = read_variants_json(spark, f'{srcdir}/{dataset}.{phenotype}.json.zst')
    bad_df = None
    for filter_to_run in filters_to_run:
        new_bad_df, df = filter_to_run.split(df)
        new_bad_df = new_bad_df.withColumn('filter_reason', lit(filter_to_run.column_name))
        bad_df = new_bad_df if bad_df is None else bad_df.union(new_bad_df)

    write_variant_json(bad_df, qcdir)
    write_variant_json(df, outdir)
    copy_metadata(srcdir, outdir)

    # done
    spark.stop()
