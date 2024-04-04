#!/usr/bin/python3
import os

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, concat_ws, explode, regexp_replace, row_number, split
from pyspark.sql.window import Window

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('vep').getOrCreate()

    # load the dbSNP database for GRCh37
    df = spark.read.csv(
        f'{s3_in}/raw/dbSNP_common_GRCh37.vcf.gz',
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

    # output the common data in CSV format (for other systems to use)
    df.write.mode('overwrite').csv(f'{s3_out}/out/varianteffect/snp', sep='\t', header=True)

    # done
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
