#from _typeshed import StrPath
#import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import rank
from pyspark.sql.window import Window

from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def main():
    """
    Arguments: dataset/phenotype
    """
    #opts = argparse.ArgumentParser()
    #opts.add_argument('path')

    # parse command line
    #args = opts.parse_args()

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load and output directory
    variant_dir = f's3://dig-analysis-data/variants/*/*/*/part-*'
    out_dir = f's3://dig-bio-index/regionmembership/'

    #CHROM   START   END
    #1       9965    10618
    #1       10647   10777
    region_dir = 's3://dig-analysis-data/out/ldsc/regions/merged/*/*.csv'

    #"varId":"1:1118275:C:T"
    variants = spark.read.json(variant_dir)
    #drop everything except 'varId'    
    variants = variants.select([c for c in variants.columns if c not in {'varId'}])

    region_schema = StructType([
      StructField("chrom", StringType(), True),
      StructField("start", IntegerType(), True),
      StructField("end", IntegerType(), True)])

    regions = spark.read.csv(region_dir,header=False,schema=region_schema)

    within = lambda i, start, end: i >= start and i <= end

    def within_region_f(row):
      (chrom, posString, ref, alt) = row['varId'].split(':')

      chrom == row['chrom'] and within(int(posString), row['start'], row['end'])

    within_region = lambda(row): within_region_f(row)

    crossed = variants.crossJoin(regions).filter(within_region)

    # write associations sorted by locus, merge into a single file
    crossed.write \
        .mode('overwrite') \
        .json('%s' % (out_dir))

    # done
    spark.stop()


if __name__ == '__main__':
    main()
