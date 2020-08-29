# imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, explode, when, lit, array_max, array, split, regexp_replace

# EC2 development localhost directories
variant_srcdir = 's3://dig-analysis-data/out/varianteffect/snp/'
out_dir = 's3://dig-analysis-data/out/magma/step1GatherVariants'

# development localhost directories
# variant_srcdir = '/Users/mduby/Data/Broad/Magma/Snp/'
# out_dir = '/Users/mduby/Data/Broad/Magma/Out/Step1'

# localhost development localhost directories
# variant_srcdir = '/home/javaprog/Data/Broad/Magma/Snp/'
# out_dir = '/home/javaprog/Data/Broad/Magma/Out/Step1'

# print
print("the variant input directory is: {}".format(variant_srcdir))
print("the output directory is: {}".format(out_dir))

# open spark session
spark = SparkSession.builder.appName('magma01').getOrCreate()
print("got Spark session of type {}".format(type(spark)))

# method to load the variants
def load_rsids(input_srcdir):
    # load the variants
    return spark.read \
        .csv('%s/part-*' % (input_srcdir), sep='\t', header=True) \
        .select('varId', 'dbSNP')# method to load the rdIds

# print
df_load = load_rsids(variant_srcdir)
print("the loaded variant data frame has {} rows".format(df_load.count()))
df_load.show()
        
# keep only the rows with non null dbSNP ids
df_nonnull_load = df_load.filter(col("dbSNP").isNotNull())

# print
print("the non null RS id dataframe has {} rows".format(df_nonnull_load.count()))
df_nonnull_load.show()

# print out distincts - /snp directory is only common variants, so less than /Common aggregate
# df_distinct = df_nonnull_load.filter(df_nonnull_load.dbSNP.isNotNull()).select(df_nonnull_load.dbSNP).distinct()
# print("the non null dsitinct RS id dataframe has {} rows".format(df_distinct.count()))

# decompose first field and get chrom/pos
split_col = split(df_nonnull_load['varId'], ':')

# add the first two columns back in
df_nonnull_load = df_nonnull_load.withColumn('chromosome', split_col.getItem(0))
df_nonnull_load = df_nonnull_load.withColumn('position', split_col.getItem(1))
df_nonnull_load.show()


# build out data frame and save magma variant input file
df_export = df_nonnull_load.select("dbSnp", 'chromosome', 'position')
df_export.count()


# replace the X/Y chromosome values with 23/24
df_export = df_export.withColumn('chromosome', regexp_replace('chromosome', 'X', '23'))
df_export = df_export.withColumn('chromosome', regexp_replace('chromosome', 'Y', '24'))
print("got df with X/Y of non null rsids to export")
df_export.count()

# show the data
df_export.printSchema()

# show the counts
df_export.groupBy("chromosome").count().orderBy("chromosome").show(25, False)
df_export = df_export.filter(col("chromosome") != 'MT')

# show the counts
df_export.groupBy("chromosome").count().orderBy("chromosome").show(25, False)

# write out the tab delimited file
# df_export.coalesce(1).write.mode('overwrite').option("delimiter", "\t").csv(out_dir)
df_export.coalesce(1).write.mode('overwrite').csv(out_dir, sep="\t", header=False)
print("wrote out {} record to file {}".format(df_export.count(), out_dir))

# stop spark
spark.stop()

# write by chromosome - not needed for 64gig mem VM
# chrom_list = list(range(1, 23))
# chrom_list.append('X')
# chrom_list.append('Y')
# for chrom in chrom_list:
#     df_write = df_export.filter(col('chromosome') == chrom)
#     # write out the tab delimited file
#     print("chrom {} has size {}".format(chrom, df_write.count()))
#     df_write.write.mode('overwrite').option("delimiter", "\t").csv(out_dir + "/" + str(chrom))


# write out by chrom
# df_export.write.mode('overwrite').option("delimiter", "\t").partitionBy("chromosome").saveAsTable(out_dir)




# example

#    by_phenotype.drop(['rank', 'top']) \
#         .orderBy(['phenotype', 'pValue']) \
#         .write \
#         .mode('overwrite') \
#         .json('%s/phenotype' % outdir)

