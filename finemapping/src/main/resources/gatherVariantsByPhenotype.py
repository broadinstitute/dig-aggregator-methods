# imports
# from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, IntegerType
# from pyspark.sql.functions import col, struct, explode, when, lit, array_max, array, split, regexp_replace
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, input_file_name, regexp_extract

def main():
    """
    Arguments: phenotype
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')

    # parse command line
    args = opts.parse_args()

    # input and output directories
    # dir_s3 = f'/Users/mduby/Data/Broad/dig-analysis-data/out'
    # dir_s3 = f'/home/javaprog/Data/Broad/dig-analysis-data/out'
    dir_s3 = f's3://dig-analysis-data/out'
    dir_snp = f'{dir_s3}/varianteffect/snp'
    dir_meta = f'{dir_s3}/metaanalysis/ancestry-specific/{args.phenotype}/*'
    dir_frequency = f'{dir_s3}/finemapping/variant-frequencies'
    dir_out = f'{dir_s3}/finemapping/variant-associations/{args.phenotype}'

    # start spark
    spark = SparkSession.builder.appName('cojo').getOrCreate()

    # load the snps
    df_snp = spark.read.csv(f'{dir_snp}/*.csv', sep='\t', header=True)
    # print("got snps df of size {}".format(df_snp.count()))
    # df_snp.show()

    # load variants and phenotype associations
    df_meta = spark.read.json(f'{dir_meta}/part-*') \
        .withColumn('filename', input_file_name()) \
        .withColumn('ancestry', regexp_extract('filename', r'/ancestry=([^/]+)/', 1))    
    # print("got metaanalysis df of size {}".format(df_meta.count()))
    # df_meta.show()

    # join pValue and snps; filter columns
    df_meta = df_meta.join(df_snp, on=['varId'], how='inner')
    df_meta = df_meta.select(df_meta.varId, 
                    df_meta.dbSNP, 
                    df_meta.alt, 
                    df_meta.reference, 
                    df_meta.stdErr, 
                    df_meta.pValue, 
                    df_meta.beta,
                    df_meta.n, 
                    df_meta.chromosome, 
                    df_meta.ancestry, 
                ) \
                .filter(df_meta.pValue < 0.001)
    # print("got joined metaanalysis df of size {}".format(df_meta.count()))
    # df_meta.show()

    # load the frequencies
    df_frequency = spark.read.json(f'{dir_frequency}/part-*')
    # print("got frequency df of size {}".format(df_frequency.count()))
    # df_frequency.show()

    # join pValue and snps; filter columns
    df_meta = df_meta.join(df_frequency, on=['varId'], how='inner')
    df_meta = df_meta.select(
            df_meta.dbSNP, 
            df_meta.alt, 
            df_meta.reference, 
            df_meta.maf, 
            df_meta.beta,
            df_meta.stdErr, 
            df_meta.pValue, 
            df_meta.n, 
            df_meta.ancestry, 
        )
    # print("got joined frequency df of size {}".format(df_meta.count()))
    # df_meta.show()

    # write out the file
    df_meta \
        .write \
        .mode('overwrite') \
        .partitionBy('ancestry') \
        .csv(dir_out, sep='\t', header='true')
    # print("wrote out data to directory {}".format(dir_out))


    # columns for COJO are:
    # - SNP 
    # - A1 - the effect allele (alt)
    # - A2 - the other allele (ref) 
    # freq - frequency of the effect allele 
    # b - effect size
    # - se - standard error
    # - p - p-value 
    # - N - sample size

    # done
    spark.stop()


if __name__ == '__main__':
    main()
