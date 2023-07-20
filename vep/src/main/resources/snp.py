#!/usr/bin/python3

import argparse
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, concat_ws, explode, regexp_replace, row_number, split
from pyspark.sql.window import Window

# where in S3 VEP data (input and output) is
S3DIR = 's3://dig-analysis-data/out/varianteffect'


def get_snp_df(spark, fname):
    # load the dbSNP database for GRCh37
    df = spark.read.csv(
        fname,
        sep='\t',
        header=False,
        comment='#',
    )

    # name the columns for easy use
    chrom = df[0]
    pos = df[1]
    rsid = df[2]
    ref = df[3]
    alt = df[4]

    # extract only the columns we care about, split multi-allelics into bi-allelics
    df = df.select(
        rsid.alias('dbSNP'),
        chrom.alias('chrom'),
        pos.alias('pos'),
        ref.alias('ref'),
        explode(split(alt, ',')).alias('alt'),
    )

    # create the variant id column
    varId = concat_ws(':', df.chrom, df.pos, df.ref, df.alt)

    # keep just the two columns
    df = df.select(df.dbSNP, varId.alias('varId'))

    # Remove duplicated varId by choosing lowest rsID
    df = df.withColumn('rsInt', regexp_replace('dbSNP', 'rs', '').cast('int'))
    w = Window.partitionBy('varId').orderBy(col('rsInt').asc())
    df = df.withColumn('row', row_number().over(w))
    df = df[df.row == 1].drop('row', 'rsInt')
    return df


def main():
    opts = argparse.ArgumentParser()
    opts.add_argument('--fname', required=True)
    opts.add_argument('--output', required=True)
    args = opts.parse_args()

    spark = SparkSession.builder.appName('vep').getOrCreate()

    df = get_snp_df(spark, f's3://dig-analysis-data/raw/{args.fname}')
    # output the common data in CSV format (for other systems to use)
    df.write.mode('overwrite').csv(f'{S3DIR}/{args.output}', sep='\t', header=True)

    # done
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
