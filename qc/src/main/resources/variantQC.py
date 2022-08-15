#!/usr/bin/python3

import argparse
import platform

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType

s3dir = 's3://dig-analysis-data'
testdir = 's3://psmadbec-test'  # Remove once we are happy with things

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
    StructField('pValue', FloatType(), nullable=False),
    StructField('beta', FloatType(), nullable=True),
    StructField('oddsRatio', FloatType(), nullable=True),
    StructField('eaf', FloatType(), nullable=True),
    StructField('maf', FloatType(), nullable=True),
    StructField('stdErr', FloatType(), nullable=True),
    StructField('zScore', FloatType(), nullable=True),
    StructField('n', FloatType(), nullable=True),
])


def read_variants_json(spark, srcdir):
    return spark.read.json(srcdir, schema=variants_schema)


def write_variant_json(df, outdir):
    df.write\
        .mode('overwrite') \
        .option("ignoreNullFields", "false")\
        .json(outdir)


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

    def save_bad(self, outdir, spark_df):
        write_variant_json(spark_df, f'{outdir}/bad_{self.column_name}')


good_chromosome = "^([1-9]{1}|1[0-9]{1}|2[0-4]{1}|X|Y)$"  # 1-24 + X + Y are valid
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

    # get the source and output directories (method_dataset is formatted as method/dataset here)
    srcdir = f'{s3dir}/variants/{args.method_dataset_phenotype}'
    outdir = f'{testdir}/out/qc/variants/{args.method_dataset_phenotype}'

    # create a spark session and dataframe from part files
    spark = SparkSession.builder.appName('qc').getOrCreate()
    df = read_variants_json(spark, f'{srcdir}/part-*')
    for filter_to_run in filters_to_run:
        bad_df, df = filter_to_run.split(df)
        filter_to_run.save_bad(outdir, bad_df)

    write_variant_json(df, outdir + '/clean')

    # done
    spark.stop()
