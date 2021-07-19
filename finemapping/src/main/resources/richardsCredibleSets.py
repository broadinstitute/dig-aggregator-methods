from pyspark.sql import SparkSession, Row
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import col, input_file_name, regexp_extract, concat
from pyspark.sql.functions import lit
import argparse
import functools


def unionAll(dfs):
    return functools.reduce(lambda df1,df2: df1.union(df2.select(df1.columns)), dfs)

def main():
    # input and output directories
    dir_s3 = f's3://dig-analysis-data/out'
    # dir_s3 = f'/Users/mduby/Data/Broad/dig-analysis-data/out'
    # dir_s3 = f'/home/javaprog/Data/Broad/dig-analysis-data/out'
    dir_results = f'{dir_s3}/finemapping/cojo-results'
    dir_out = f'{dir_s3}/finemapping/richards-credible-set-results'

    # start spark
    spark = SparkSession.builder.appName('cojo').getOrCreate()

    # load the lead snps
    df_lead_snp = spark.read.csv(f'{dir_results}/*/*/*.jma.cojo', sep='\t', header=True) \
        .withColumn('filename', input_file_name()) \
        .withColumn('ancestry', regexp_extract('filename', r'/ancestry=([^/]+)/', 1)) \
        .withColumn('pheno', regexp_extract('filename', r'/([^/]+)/ancestry=', 1))
    print("got lead snp df of size {}".format(df_lead_snp.count()))
    df_lead_snp.groupBy('ancestry', 'pheno').count().show(70)

    # add extra conditional columns as null
    df_lead_snp = df_lead_snp \
        .withColumn('bC', lit(None).cast(StringType())) \
        .withColumn('bC_se', lit(None).cast(StringType())) \
        .withColumn('pC', lit(None).cast(StringType())) \
        .withColumn('signal_chromosome', df_lead_snp.Chr) \
        .withColumn('signal_position', df_lead_snp.bp) \
        .withColumn('credible_set', df_lead_snp.SNP)
    print("have column types \n{}".format(df_lead_snp.dtypes))
    print(df_lead_snp.show(20))


    # df_lead_snp = df_lead_snp.filter(df_lead_snp.bp < 2885636).filter(df_lead_snp.bp > 2808400).filter(df_lead_snp.Chr.isin(11)).filter(df_lead_snp.pheno.isin('T2D'))
    # df_lead_snp.groupBy('ancestry', 'pheno').count().show(70)

    
    df_lead_snp = df_lead_snp.select(
        df_lead_snp.SNP.alias('dbSNP'),
        df_lead_snp.Chr.alias('chromosome'),
        df_lead_snp.bp,
        df_lead_snp.bp.alias('position').cast(IntegerType()),
        df_lead_snp.refA.alias('ref'),
        df_lead_snp.freq.alias('maf'),
        df_lead_snp.ancestry,
        df_lead_snp.pheno,
        df_lead_snp.p.alias('pValue'),
        df_lead_snp.pJ.alias('pValueNew'),
    ).sort(df_lead_snp.bp)
    print("got lead snp df of size {}".format(df_lead_snp.count()))
    print(df_lead_snp.printSchema())

    # add column of lead snp
    # print(df_lead_snp.show(20))


    # sort by pValue, build distinct snp array and dataframe 
    variants = df_lead_snp.orderBy(['pValueNew']).collect()   # actually run plan
    topVariants = []
    bottomVariants = []
    count = 0
    while variants:
        best = variants[0]
        temp = best.asDict()
        temp['lead_snp'] = best['chromosome'] + "_" + best['bp'] + "_" + best['pheno'] + "_"+ best['ancestry']
        temp['start_position'] = best['position'] - 5000000
        temp['end_position'] = best['position'] + 5000000
        topVariants.append(Row(**temp))

        # remove all variants around best and put them into the other array
        tempArray = [v for v in variants if (abs(v['position'] - best['position']) <= 10000000) and (v['position'] != best['position']) and (v['chromosome'] == best['chromosome']) and (v['pheno'] == best['pheno'])]
        for row in tempArray:
            temp = row.asDict()
            temp['lead_snp'] = best['chromosome'] + "_" + best['bp'] + "_"+ best['pheno'] + "_"+ best['ancestry']
            newRow = Row(**temp)
            bottomVariants.append(newRow)

        # bottomVariants += tempArray
        # if count == 0:
        #     print("bottom {}".format(bottomVariants))
        count += count
        variants = [v for v in variants if (abs(v['position'] - best['position']) > 10000000) or (v['chromosome'] != best['chromosome']) or (v['pheno'] != best['pheno'])]

    # make a new dataframe with the resulting top variants
    df_new_lead_snp = spark.createDataFrame(topVariants)
    df_not_lead_snp = spark.createDataFrame(bottomVariants)
    print("got lead snp df of size {} and non lead snp of size {}".format(df_new_lead_snp.count(), df_not_lead_snp.count()))
    print(df_new_lead_snp.show(20))


    # # load the conditioned snps
    df_conditioned_snp = spark.read.csv(f'{dir_results}/*/*/*.cma.cojo', sep='\t', header=True) \
        .withColumn('filename', input_file_name()) \
        .withColumn('ancestry', regexp_extract('filename', r'/ancestry=([^/]+)/', 1)) \
        .withColumn('pheno', regexp_extract('filename', r'/([^/]+)/ancestry=', 1))
    print("got conditioned snp df of size {}".format(df_conditioned_snp.count()))
    df_conditioned_snp.groupBy('ancestry').count().show(20)

    # alias the columns to standard
    df_conditioned_snp = df_conditioned_snp.select(
        df_conditioned_snp.SNP.alias('dbSNP'),
        df_conditioned_snp.Chr.alias('chromosome'),
        df_conditioned_snp.bp,
        df_conditioned_snp.bp.alias('position').cast(IntegerType()),
        df_conditioned_snp.refA.alias('ref'),
        df_conditioned_snp.freq.alias('maf'),
        df_conditioned_snp.ancestry,
        df_conditioned_snp.pheno,
        df_conditioned_snp.p.alias('pValue'),
        df_conditioned_snp.pC.alias('pValueNew'))

    # join with the new lead snps with the conditioned snps
    df_joined_conditioned_snp = df_conditioned_snp.join(df_new_lead_snp, 
        [df_conditioned_snp.chromosome == df_new_lead_snp.chromosome, 
            df_conditioned_snp.pheno == df_new_lead_snp.pheno,
            df_conditioned_snp.ancestry == df_new_lead_snp.ancestry,
            df_conditioned_snp.position > df_new_lead_snp.start_position,
            df_conditioned_snp.position < df_new_lead_snp.end_position,
        ], how='inner').select(
        df_conditioned_snp.dbSNP,
        df_conditioned_snp.chromosome,
        df_conditioned_snp.bp,
        df_conditioned_snp.position,
        df_conditioned_snp.ref,
        df_conditioned_snp.maf,
        df_conditioned_snp.ancestry,
        df_conditioned_snp.pheno,
        df_conditioned_snp.pValue,
        df_conditioned_snp.pValueNew,
        concat(df_new_lead_snp.chromosome, lit("_"), df_new_lead_snp.position, lit("_"), df_new_lead_snp.pheno, lit("_"), df_new_lead_snp.ancestry).alias('lead_snp')
    )
    print("got new conditioned snp df of size {}".format(df_joined_conditioned_snp.count()))
    df_joined_conditioned_snp.show(10)
    df_joined_conditioned_snp.groupBy('lead_snp').count().show(20)

    # drop the extra lean snp columns
    columns_to_drop = ['start_position', 'end_position']
    df_new_lead_snp = df_new_lead_snp.drop(*columns_to_drop)

    # union all dataframes
    df_all_snp = unionAll([df_new_lead_snp, df_not_lead_snp, df_joined_conditioned_snp])
    print("got all snp df of size {}".format(df_all_snp.count()))
    df_all_snp.show(10)
    df_all_snp.groupBy(['lead_snp']).count().show(10)

    # write out
    df_all_snp \
        .orderBy(df_all_snp.lead_snp, df_all_snp.position) \
        .write \
        .mode('overwrite') \
        .json('%s' % dir_out)
    print("wrote out data to directory {}".format(dir_out))

# got all snp df of size 2326041                                                  
# +-----------+----------+---------+---------+---+------+--------+-----+-----------+-----------+-------------------+
# |      dbSNP|chromosome|       bp| position|ref|   maf|ancestry|pheno|     pValue|  pValueNew|           lead_snp|
# +-----------+----------+---------+---------+---+------+--------+-----+-----------+-----------+-------------------+
# |  rs9274247|         6| 32631332| 32631332|  A|0.1983|      EU|  T2D|4.57958e-12|          0|  6_32631332_T2D_EU|
# | rs11786992|         8| 95685147| 95685147|  C|0.3393|      EU|  T2D|1.76943e-15|1.00241e-09|  8_95685147_T2D_EU|
# |  rs2149992|         9|128738591|128738591|  A|0.4605|      EA|  T2D|6.27739e-08| 1.0037e-12| 9_128738591_T2D_EA|
# | rs16895917|         4| 17869234| 17869234|  C|0.0443|      EA|  T2D|1.00591e-08|1.00644e-08|  4_17869234_T2D_EA|
# |  rs4287864|        22| 41451953| 41451953|  C|0.1476|      EA|  T2D|1.34022e-09|1.00657e-08| 22_41451953_T2D_EA|
# |  rs2706710|        17| 65641651| 65641651|  T|0.2294|      EA|  T2D|1.37093e-12|1.00807e-10| 17_65641651_T2D_EA|
# | rs11071759|        15| 63922474| 63922474|  C|0.4169|      EU|  T2D|7.07418e-14|1.00819e-16| 15_63922474_T2D_EU|
# |  rs2816941|         1|199990779|199990779|  G|0.2756|      EU|  T2D|1.12271e-09|1.00866e-09| 1_199990779_T2D_EU|
# |  rs1635851|         7| 28187806| 28187806|  T|0.4417|      EU|  T2D|1.34575e-52|1.00877e-55|  7_28187806_T2D_EU|



# # cojo headers
# # +---+-----------+---------+----+------+---------+---------+-----------+-------+---------+---------+---------+------------+-----------+--------------------+--------+---------+---------+-----------+
# # |Chr|        SNP|       bp|refA|  freq|        b|       se|          p|      n|freq_geno|       bJ|    bJ_se|          pJ|       LD_r|            filename|ancestry|       bC|    bC_se|         pC|
# # +---+-----------+---------+----+------+---------+---------+-----------+-------+---------+---------+---------+------------+-----------+--------------------+--------+---------+---------+-----------+

#     # write out the file
#     df_all_snp \
#         .write.mode('overwrite') \
#         .json(dir_out)
#     print("wrote out data to directory {}".format(dir_out))


    # done
    spark.stop()


if __name__ == "__main__":
    main()