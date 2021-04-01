from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, input_file_name, regexp_extract

def main():
    # input and output directories
    # dir_s3 = f'/Users/mduby/Data/Broad/dig-analysis-data/out'
    # dir_s3 = f'/home/javaprog/Data/Broad/dig-analysis-data/out'
    dir_s3 = f's3://dig-analysis-data/out'
    dir_frequency = f'{dir_s3}/varianteffect/common'
    dir_out = f'{dir_s3}/finemapping/variant-frequencies'

    # start spark
    spark = SparkSession.builder.appName('cojo').getOrCreate()

    # load the frequencies
    df_frequency = spark.read.json(f'{dir_frequency}/part-*')
    # print("got frequency df of size {}".format(df_frequency.count()))
    df_frequency = df_frequency \
        .filter(df_frequency.maf.isNotNull()) \
        .select(
            df_frequency.varId,
            df_frequency.maf,
        )
    # print("got null filtered frequency df of size {}".format(df_frequency.count()))


    # write out the file
    df_frequency \
        .write.mode('overwrite') \
        .json(dir_out)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
