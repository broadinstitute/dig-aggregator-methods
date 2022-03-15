# imports
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, input_file_name, regexp_extract, lit

def main():
    """
    Arguments: phenotype
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype_arg')

    # parse command line
    args = opts.parse_args()
    phenotype_arg = args.phenotype_arg
    print("got phenotype argument: {}".format(phenotype_arg))

    # input and output directories
    # dir_s3 = f'/Users/mduby/Data/Broad/dig-analysis-data/out'
    # dir_s3 = f'/home/javaprog/Data/Broad/dig-analysis-data/out'
    dir_s3 = f's3://dig-analysis-data/out'
    dir_snp = f'{dir_s3}/varianteffect/snp'
    dir_largest_datasets = f'{dir_s3}/finemapping/largest-datasets'
    dir_frequency = f'{dir_s3}/finemapping/variant-frequencies'
    dir_out = "{}/finemapping/variant-associations-largest-datasets/{}"

    # start spark
    spark = SparkSession.builder.appName('cojo').getOrCreate()

    # load the snps
    df_snp = spark.read.csv(f'{dir_snp}/*.csv', sep='\t', header=True)
    print("got snps df of size {}".format(df_snp.count()))
    df_snp.show(5)

    # load the frequencies
    df_frequency = spark.read.json(f'{dir_frequency}/part-*')
    print("got frequency df of size {}".format(df_frequency.count()))
    df_frequency.show(5)

    # load the phenotype/ancestry dataset list data
    path_load = f'{dir_largest_datasets}/{phenotype_arg}/part-*'
    print("loading dataset listings from path: {}".format(path_load))
    df_largest_dataset = spark.read.json(path_load)
    # df_largest_dataset = spark.read.json(f'{dir_largest_datasets}/../largest-datasets-test/test.json')
    print("got largest dataset dataframe of size {}".format(df_largest_dataset.count()))
    df_largest_dataset.show(5)

    # DEPRECATED - for each row where the count of datasets is greater than 1, use the largest dataset to get phenotype association stats
    # df_largest_dataset_more_than_one_dataset = df_largest_dataset.filter(col("count") > 1).collect()
    # print("got dataset more than 1 df of size {}".format(len(df_largest_dataset_more_than_one_dataset)))

    # only run if have at least one dataset (safety check)
    if df_largest_dataset.count() > 1:
        # collect for loop; will not filter for datasets more than one since collecting fifferent MAF values than for bottom line
        df_largest_dataset_more_than_one_dataset = df_largest_dataset.collect()
        is_dff_exist = False
        for row in df_largest_dataset_more_than_one_dataset:
            phenotype = row['phenotype']
            ancestry = row['ancestry']
            directory = row['directory']
            print("for {}/{} got directory {}".format(phenotype, ancestry, directory))

            # load variants and phenotype associations
            df_meta = spark.read.json(f'{directory}/part-*') \
                .withColumn('ancestry', lit(ancestry))
            df_meta = df_meta.select(
                            df_meta.varId, 
                            df_meta.alt, 
                            df_meta.reference, 
                            df_meta.beta,
                            df_meta.stdErr, 
                            df_meta.pValue, 
                            df_meta.n, 
                            df_meta.ancestry, 
                        )
            print("got {}/{} metaanalysis df of size {}".format(phenotype, ancestry, df_meta.count()))
            # df_meta.show()

            # need to concatenate all ancestry dataframes before joining with df_snp
            if not is_dff_exist:
                dff = df_meta
                is_dff_exist = True
            else:
                dff = dff.unionAll(df_meta)
            print("got combined ancestry metaanalysis df of size {}".format(dff.count()))

        # join pValue and snps; filter columns
        df_meta = dff.join(df_snp, on=['varId'], how='inner')
        df_meta = df_meta.select(
                df_meta.varId,
                df_meta.dbSNP, 
                df_meta.alt, 
                df_meta.reference, 
                df_meta.beta,
                df_meta.stdErr, 
                df_meta.pValue, 
                df_meta.n, 
                df_meta.ancestry, 
            )
        print("got joined metaanalysis/snp df of size {}".format(df_meta.count()))
        df_meta.show(5)

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
        print("got joined frequency df of size {}".format(df_meta.count()))
        df_meta.show(5)

        # filter for pvalue threshold
        p_value_limit = 0.01
        df_meta = df_meta.filter(df_meta.pValue < p_value_limit)
        print("got {} pValue joined frequency df of size {}".format(p_value_limit, df_meta.count()))
        df_meta.show(5)

        # write out the file
        dir_pheno_out = dir_out.format(dir_s3, phenotype)
        df_meta \
            .write \
            .mode('overwrite') \
            .partitionBy('ancestry') \
            .csv(dir_pheno_out, sep='\t', header='true')
        print("wrote out data to directory {}".format(dir_pheno_out))


    else:
        print("no datasets to process, so skip")
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
