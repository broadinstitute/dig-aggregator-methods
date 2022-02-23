# imports
# from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, IntegerType
# from pyspark.sql.functions import col, struct, explode, when, lit, array_max, array, split, regexp_replace
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, input_file_name, regexp_extract, max, input_file_name, regexp_replace

def main():
    # input and output directories
    # dir_s3 = f'/Users/mduby/Data/Broad/dig-analysis-data'
    # dir_s3 = f'/home/javaprog/Data/Broad/dig-analysis-data'
    dir_s3 = f's3://dig-analysis-data'
    dir_meta = f'{dir_s3}/variants/*/*/*'
    dir_out = f'{dir_s3}/out/finemapping/largest-datasets'

    # start spark
    spark = SparkSession.builder.appName('cojo').getOrCreate()

    # load variants and phenotype associations
    df_meta = spark.read.json(f'{dir_meta}/metadata').withColumn("directory", input_file_name())
    print("got metaanalysis df of size {}".format(df_meta.count()))
    df_meta.show()
    print("dataframe of type: {}".format(type(df_meta)))

    # get distinct phenotypes
    print("got {} distinct phenotypes".format(df_meta.select("phenotype").distinct().count()))

    # get the distinct ancestries
    df_meta.groupBy("ancestry").count().show()

    # replace AA with AF
    df_meta = df_meta.withColumn('ancestry', regexp_replace('ancestry', 'AA', 'AF'))

    # remove Mixed
    df_meta = df_meta.where(col("ancestry").isin("EA", "AF", "EU", "SA", "HS"))
    df_meta.groupBy("ancestry").count().show()

    # TODO - create new dataset with phenotype/ethinicity combination and the dataset count
    # find the ancestry/phenotype combinations that only have one dataset -> mark those so trans ehtnic cojo doesn't duplicate calculation
    df_combo_count = df_meta.groupBy("phenotype", "ancestry").count()
    df_combo_count.show()
    print("got {} counted phenotypes/ethnicity counts".format(df_meta.select("phenotype").distinct().count()))

    # find the max subjects per ethnicity/phenotype combination
    df_max_subjects = df_meta.groupBy("phenotype", "ancestry").agg(max("subjects").alias("subjects"))
    df_max_subjects.show()

    # for count > 1, identify the largest dataset and store in field for that row
    df_max_subjects = df_max_subjects.join(df_meta, on=["phenotype", "ancestry", "subjects"], how="inner")
    df_max_subjects = df_max_subjects.select("phenotype", "ancestry", "subjects", "directory")
    df_max_subjects = df_max_subjects.withColumn("directory", regexp_replace(col("directory"), "metadata", ""))
    # (df_max_subjects.phenotype == df_meta.phenotype) & \
    #     (df_max_subjects.ancestry == df_meta.ancestry) & (df_max_subjects.max_subjects == df_meta.subjects), "inner") 
        # .select("ancestry", "phenotype", "subjects", "sources")
    df_max_subjects.show()

    # add the count for each pheno/ancestry combo 
    # this will determine whether to copy bottom line results or compute again on the single dataset
    df_max_subjects = df_max_subjects.join(df_combo_count, on=["phenotype", "ancestry"], how="inner")
    df_max_subjects.show()
    print("got {} rows of pheno/ancestry combinations".format(df_max_subjects.count()))
    df_toss = df_max_subjects.filter(col("count") > 1)
    print("got {} rows of pheno/ancestry combinations over 1".format(df_toss.count()))

    # test for one phenotype/ancestry combination
    # df_fg = df_meta.filter(col("phenotype") == 'HBA1C').filter(col("ancestry") == 'SA')
    # df_fg.show()

    # write out the file for tracking purposes
    df_max_subjects \
        .coalesce(1) \
        .write \
        .mode('overwrite') \
        .partitionBy('phenotype') \
        .json(dir_out)
    print("wrote out data to directory {}".format(dir_out))
        # .partitionBy('phenotype', 'ancestry') \
        # .csv(dir_out, sep='\t', header='true')

    # done
    spark.stop()

if __name__ == '__main__':
    main()
