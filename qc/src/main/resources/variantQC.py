#!/usr/bin/python3

import argparse
import platform

from pyspark.sql import SparkSession

s3dir = 's3://dig-analysis-data'
testdir = 's3://psmadbec-test'  # Remove once we are happy with things


class VariantColumnFilter:
    def __init__(self, column_name, regex_pattern, nullable=True):
        self.column_name = column_name
        self.regex_pattern = regex_pattern
        self.nullable = nullable

    def apply_nullable(self, spark_df):
        if self.nullable:
            return spark_df[self.column_name].rlike(self.regex_pattern) | \
                   spark_df[self.column_name].isNull()
        else:
            return spark_df[self.column_name].rlike(self.regex_pattern) & \
                   ~spark_df[self.column_name].isNull()

    def split(self, spark_df):
        condition = self.apply_nullable(spark_df)
        return spark_df.filter(~condition), spark_df.filter(condition)

    def save_bad(self, outdir, spark_df):
        spark_df.write \
            .mode('overwrite') \
            .option("ignoreNullFields", "false") \
            .json(f'{outdir}/bad_{self.column_name}')


good_chromosome = "^([1-9]{1}|1[0-9]{1}|2[0-2]{1}|X|Y)$"  # 1-22 + X + Y are valid
good_integer = "^[0-9]+$"
good_float = "^-?[0-9]+.?[0-9]*[eE]?-?[0-9]*$"  # includes scientific notation and signed
good_base = "^[atcgATCG]+$"  # case insensitive, only ATCG, no multialleles

filters_to_run = [
    VariantColumnFilter("chromosome", good_chromosome, nullable=False),
    VariantColumnFilter("position", good_integer, nullable=False),
    VariantColumnFilter("reference", good_base, nullable=False),
    VariantColumnFilter("alt", good_base, nullable=False),
    VariantColumnFilter("pValue", good_float, nullable=False),
    VariantColumnFilter("oddsRatio", good_float),
    VariantColumnFilter("beta", good_float),
    VariantColumnFilter("stdErr", good_float),
    VariantColumnFilter("eaf", good_float),
    VariantColumnFilter("n", good_float, nullable=False)
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
    df = spark.read.json(f'{srcdir}/part-*')
    for filter_to_run in filters_to_run:
        bad_df, df = filter_to_run.split(df)
        filter_to_run.save_bad(outdir, bad_df)

    # output the variants as json (identical to the input is there is no filtering)
    df.write \
        .option("ignoreNullFields", "false") \
        .mode('overwrite') \
        .json(outdir + '/clean')

    # done
    spark.stop()
