# imports
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, IntegerType
from pyspark.sql.functions import col, struct, explode, when, lit, array, udf

# load and output directory
# vep_srcdir = 's3://dig-analysis-data/out/varianteffect/effects/part-*'
# freq_srcdir = 's3://dig-analysis-data/out/frequencyanalysis/'
# outdir = 's3://dig-bio-index/burden/variantgene'

# development localhost directories
vep_srcdir = '/home/javaprog/Data/Broad/dig-analysis-data/out/varianteffect/effects/part-*'
freq_srcdir = '/home/javaprog/Data/Broad/dig-analysis-data/out/frequencyanalysis/'
outdir = '/home/javaprog/Data/Broad/dig-analysis-data/out/burdenbinning/results'

# print
# print("the input directory is: {}".format(vep_srcdir))

# open spark session
spark = SparkSession.builder.appName('burdenbinning').getOrCreate()

# constants for filters
# there are 3 levels of filters (lof, impact + maf, and combined predictions)
# the 7 bins will combine variantions of these three OR conditions

# general filter
filter_pick = "pick"

# level 1 filter
filter_lof = "lof"

# level 2 filters
filter_polyphen2_hdiv_pred = "polyphen2_hdiv_pred"
filter_polyphen2_hvar_pred = "polyphen2_hvar_pred"
filter_sift_red = "sift_pred"
filter_mutationtaster_pred = "mutationtaster_pred"
filter_lrt_pred = "lrt_pred"
filter_metalr_pred = "metalr_pred"
filter_provean_pred = "provean_pred"
filter_fathmm_pred = "fathmm_pred"
filter_fathmm_mkl_coding_pred = "fathmm-mkl_coding_pred"
filter_eigen_pc_raw_rankscore = "eigen-pc-raw_rankscore"
filter_dann_rankscore = "dann_rankscore"
filter_vest3_rankscore = "vest3_rankscore"
filter_cadd_raw_rankscore = "cadd_raw_rankscore"
filter_metasvm_pred = "metasvm_pred"

# aliases w/o -
filter_fathmm_mkl_coding_pred_alias = "fathmm_mkl_coding_pred"
filter_eigen_pc_raw_rankscore_alias = "eigen_pc_raw_rankscore"

# level 3 filter
filter_impact = "impact"

# column constants
var_id = "varId"
gene_ensemble_id = "ensemblId"
burden_bin_id = "burdenBinId"
maf = 'maf'

# column variables for output
var_id_col = col(var_id)
gene_ensemble_id_col = col(gene_ensemble_id)
burden_bin_id_col = col(burden_bin_id)
maf_col = col(maf)

# column variables for filters
filter_lof_col = col("lof")
filter_impact_col = col("impact")
filter_polyphen2_hdiv_pred_col = col("polyphen2_hdiv_pred")
filter_polyphen2_hvar_pred_col = col("polyphen2_hvar_pred")
filter_sift_pred_col = col("sift_pred")
filter_lrt_pred_col = col("lrt_pred")
filter_mutationtaster_pred_col = col("mutationtaster_pred")

filter_metalr_pred_col = col("metalr_pred")
filter_provean_pred_col = col("provean_pred")
filter_fathmm_pred_col = col("fathmm_pred")
filter_fathmm_mkl_coding_pred_col = col("fathmm_mkl_coding_pred")
filter_eigen_pc_raw_rankscore_col = col("eigen_pc_raw_rankscore")
filter_dann_rankscore_col = col("dann_rankscore")
filter_vest3_rankscore_col = col("vest3_rankscore")
filter_cadd_raw_rankscore_col = col("cadd_raw_rankscore")
filter_metasvm_pred_col = col("metasvm_pred")

# variables for filters conditions
condition_lof_hc = filter_lof_col == 'HC'
condition_impact_moderate = (filter_impact_col == 'MODERATE') & (maf_col < 0.01)
condition_impact_high = (filter_impact_col == 'HIGH') & (filter_lof_col == 'LC') & (maf_col < 0.01)
# condition_impact_moderate = filter_impact_col == 'MODERATE'
# condition_impact_high = filter_impact_col == 'HIGH'

# bin7 - level 2 condition for bin 7
condition_level2_bin7 = (~filter_polyphen2_hdiv_pred_col.contains('D')) & \
        (~filter_polyphen2_hvar_pred_col.contains('D')) & \
        (~filter_sift_pred_col.contains('D')) &  \
        (~filter_lrt_pred_col.contains('D')) & \
        (~(filter_mutationtaster_pred_col.contains('A') & filter_mutationtaster_pred_col.contains('D')))

# bin6 - level 2 exclusion condition for bin 6
condition_level2_inclusion_bin6 = (filter_polyphen2_hdiv_pred_col.contains('D')) | \
        (filter_polyphen2_hvar_pred_col.contains('D')) | \
        (filter_sift_pred_col.contains('D')) | \
        (filter_lrt_pred_col.contains('D')) | \
        (filter_mutationtaster_pred_col.isin(['A', 'D']))

# bin5 - level 2 exclusion condition for bin 5
condition_level2_inclusion_bin5 = (filter_polyphen2_hdiv_pred_col.contains('D')) & \
        (filter_polyphen2_hvar_pred_col.contains('D')) & \
        (filter_sift_pred_col.contains('D')) & \
        (filter_lrt_pred_col.contains('D')) & \
        (filter_mutationtaster_pred_col.isin(['A', 'D']))

# bin3 - level 2 inclusion condition for bin 3
condition_level2_inclusion_bin3 = condition_level2_inclusion_bin5 & \
        (filter_metalr_pred_col.contains('D')) & \
        (filter_metasvm_pred_col.contains('D')) &  \
        (filter_provean_pred_col.contains('D')) & \
        (filter_fathmm_mkl_coding_pred_col.contains('D')) & \
        (filter_fathmm_pred_col.contains('D'))

# bin2 - level 2 exclusion condition for bin 2
condition_level2_inclusion_bin2 = condition_level2_inclusion_bin3 & \
        (filter_eigen_pc_raw_rankscore_col > 0.9) & \
        (filter_dann_rankscore_col > 0.9) & \
        (filter_cadd_raw_rankscore_col > 0.9) & \
        (filter_vest3_rankscore_col > 0.9) 

# schemas for csv files
# this is the schema written out by the frequency analysis processor
frequency_schema = StructType(
    [
        StructField('varId', StringType(), nullable=False),
        StructField('chromosome', StringType(), nullable=False),
        StructField('position', IntegerType(), nullable=False),
        StructField('reference', StringType(), nullable=False),
        StructField('alt', StringType(), nullable=False),
        StructField('eaf', DoubleType(), nullable=False),
        StructField('maf', DoubleType(), nullable=False),
        StructField('ancestry', StringType(), nullable=False),
    ]
)

# functions
# method to load the frequencies
# method to load the frequencies
def load_freq(ancestry_name, input_srcdir):
    return spark.read \
        .json('%s/%s/part-*' % (input_srcdir, ancestry_name)) \
        .select(var_id_col, maf_col.alias(ancestry_name))

# method to get the max of an array
def max_array(array_var):
    max = 0.0                        # maf will never be less than 0
    for element in array_var:
        if (element is not None):
            if (element > max):
                max = element
    return max

# custom function used for sorting chromosomes properly
max_array_udf = udf(max_array, DoubleType())


# load and do the maf calculations
# frequency outputs by ancestry
# ancestries = ['AA', 'AF', 'EA', 'EU', 'HS', 'SA']
ancestries = ['AA', 'EA', 'EU', 'HS', 'SA']
dataframe_freq = None

# load frequencies by variant ID
for ancestry in ancestries:
    df = load_freq(ancestry, freq_srcdir)

    # final, joined frequencies
    dataframe_freq = df if dataframe_freq is None else dataframe_freq.join(df, var_id, how='outer')

# pull all the frequencies together into a single array
dataframe_freq = dataframe_freq.select(dataframe_freq.varId, array(*ancestries).alias('frequency'))
#
# # get the max for all frequencies
dataframe_freq = dataframe_freq.withColumn('maf', max_array_udf('frequency')).select(dataframe_freq.varId, 'maf')

# print
print("the loaded frequency data frame has {} rows".format(dataframe_freq.count()))
# dataframe_freq.show()

# load the transcript json data
vep = spark.read.json(vep_srcdir)

# print
# print("the loaded vep data count is: {}".format(vep.count()))
# format(vep.show())

# create new data frame with only var id
transcript_consequences = vep.select(vep.id, vep.transcript_consequences)     .withColumn('cqs', explode(col('transcript_consequences')))     .select(
        col('id').alias('varId'),
        col('cqs.gene_id').alias(gene_ensemble_id),
        col('cqs.' + filter_lof).alias(filter_lof),
        col('cqs.' + filter_impact).alias(filter_impact),

        col('cqs.' + filter_polyphen2_hdiv_pred).alias(filter_polyphen2_hdiv_pred),
        col('cqs.' + filter_polyphen2_hvar_pred).alias(filter_polyphen2_hvar_pred),
        col('cqs.' + filter_sift_red).alias(filter_sift_red),
        col('cqs.' + filter_mutationtaster_pred).alias(filter_mutationtaster_pred),
        col('cqs.' + filter_lrt_pred).alias(filter_lrt_pred),
        col('cqs.' + filter_metalr_pred).alias(filter_metalr_pred),

        col('cqs.' + filter_provean_pred).alias(filter_provean_pred),
        col('cqs.' + filter_fathmm_pred).alias(filter_fathmm_pred),
        col('cqs.' + filter_fathmm_mkl_coding_pred).alias(filter_fathmm_mkl_coding_pred_alias),
        col('cqs.' + filter_eigen_pc_raw_rankscore).alias(filter_eigen_pc_raw_rankscore_alias),
        col('cqs.' + filter_dann_rankscore).alias(filter_dann_rankscore),
        col('cqs.' + filter_vest3_rankscore).alias(filter_vest3_rankscore),
        col('cqs.' + filter_cadd_raw_rankscore).alias(filter_cadd_raw_rankscore),
        col('cqs.' + filter_metasvm_pred).alias(filter_metasvm_pred),
        col('cqs.transcript_id')
    )


# print
print("the filtered test data count is: {}".format(transcript_consequences.count()))
# transcript_consequences.show()

# join the transcripts dataframe with the maf dataframe
transcript_consequences = transcript_consequences.join(dataframe_freq, var_id, how='left')
print("the filtered transcript with frequency data count is: {}".format(transcript_consequences.count()))
transcript_consequences.show()

# get the lof level 1 data frame
dataframe_lof = transcript_consequences.filter(condition_lof_hc)
print("the lof data frame count is: {}".format(dataframe_lof.count()))
# dataframe_lof.show()

# get the level 3 dataframe
dataframe_impact_moderate = transcript_consequences.filter(condition_impact_moderate)
dataframe_impact_high = transcript_consequences.filter(condition_impact_high)
print("the moderate impact dataframe is {}".format(dataframe_impact_moderate.count()))
print("the high impact dataframe is {}".format(dataframe_impact_high.count()))

# BIN 1 of 7
# create the final_1 df, just lof = HC
final_bin1_data_frame = dataframe_lof.withColumn(burden_bin_id, lit('bin1_7')).distinct()
print("the final bin 1 dataframe is: {}".format(final_bin1_data_frame.count()))
# final_bin1_data_frame.show()

# BIN 7 of 7
# get the initial level 2 dataframe
dataframe_level2 = transcript_consequences.filter(condition_level2_bin7)

# create the final_7 df, lof = HC, impact moderate, add in level 2 filters
final_bin7_data_frame = dataframe_lof.union(dataframe_impact_moderate).union(dataframe_level2).distinct()
final_bin7_data_frame = final_bin7_data_frame.withColumn(burden_bin_id, lit('bin7_7'))
print("the final bin 7 dataframe is: {}".format(final_bin7_data_frame.count()))
# final_bin7_data_frame.show()

# BIN 6 of 7
# get the exclusion level 2 data frame
dataframe_level2_exclusion = transcript_consequences.filter(~condition_level2_inclusion_bin5)
dataframe_level2_inclusion = transcript_consequences.filter(condition_level2_inclusion_bin6)

# create the final_6 df, lof = HC, impact moderate, add in level 2 filters
final_bin6_data_frame = dataframe_level2_exclusion.union(dataframe_level2_inclusion) \
    .union(dataframe_lof) \
    .union(dataframe_impact_moderate) \
    .union(dataframe_level2_inclusion) \
    .distinct()
final_bin6_data_frame = final_bin6_data_frame.withColumn(burden_bin_id, lit('bin6_7'))
print("the final bin 6 dataframe is: {}".format(final_bin6_data_frame.count()))
# final_bin6_data_frame.show()

# BIN 5 of 7
# already have the inclusion level 2 data frame 
dataframe_level2_inclusion_bin5 = transcript_consequences.filter(condition_level2_inclusion_bin5)

# create the final_5 df, lof = HC, impact moderate, add in level 2 filters
final_bin5_data_frame = dataframe_lof.union(dataframe_level2_inclusion_bin5).union(dataframe_impact_high).distinct()
final_bin5_data_frame = final_bin5_data_frame.withColumn(burden_bin_id, lit('bin5_7'))
print("the final bin 5 dataframe is: {}".format(final_bin5_data_frame.count()))
# final_bin5_data_frame.show()

# BIN 4 of 7
# already have the inclusion level 2 data frame (exclusion from the previous bin 6 of 7)

# create the final_4 df, lof = HC, impact moderate, add in level 2 filters
final_bin4_data_frame = dataframe_lof.union(dataframe_level2_inclusion_bin5).distinct()
final_bin4_data_frame = final_bin4_data_frame.withColumn(burden_bin_id, lit('bin4_7'))
print("the final bin 4 dataframe is: {}".format(final_bin4_data_frame.count()))
# final_bin4_data_frame.show()

# BIN 3 of 7
# bin consists of bin4 level 2 filter with some added on filters
dataframe_bin3_level2_inclusion = transcript_consequences.filter(condition_level2_inclusion_bin3)

# create the final_3 df, lof = HC, add in level 2 filters
final_bin3_data_frame = dataframe_lof.union(dataframe_bin3_level2_inclusion).distinct()
final_bin3_data_frame = final_bin3_data_frame.withColumn(burden_bin_id, lit('bin3_7'))
print("the final bin 3 dataframe is: {}".format(final_bin3_data_frame.count()))
# final_bin7_data_frame.show()

# BIN 2 of 7
# bin consists of bin3 level 2 filter with some more added on filters
dataframe_bin2_level2_inclusion = transcript_consequences.filter(condition_level2_inclusion_bin2)

# create the final_2 df, lof = HC, add in level 2 filters
final_bin2_data_frame = dataframe_lof.union(dataframe_bin2_level2_inclusion).distinct()
final_bin2_data_frame = final_bin2_data_frame.withColumn(burden_bin_id, lit('bin2_7'))
print("the final bin 2 dataframe is: {}".format(final_bin3_data_frame.count()))
# final_bin2_data_frame.show()

# combine all the bins into one dataframe
output_data_frame = final_bin1_data_frame \
        .union(final_bin2_data_frame) \
        .union(final_bin3_data_frame) \
        .union(final_bin4_data_frame) \
        .union(final_bin5_data_frame) \
        .union(final_bin6_data_frame) \
        .union(final_bin7_data_frame).distinct()
    # .distinct() \
    # .orderBy(var_id, gene_ensemble_id, burden_bin_id)

# print
print("the final agregated bin dataframe is: {}".format(output_data_frame.count()))

# save out the output data frame to file
output_data_frame \
        .orderBy(gene_ensemble_id_col, burden_bin_id_col) \
        .write \
        .mode('overwrite') \
        .json('%s' % outdir)

# print
print("Printed out {} records to {}".format(output_data_frame.count(), outdir))

# done
# spark.stop()

