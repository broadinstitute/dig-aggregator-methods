# imports
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, IntegerType
from pyspark.sql.functions import col, struct, explode, when, lit, array_max, array, split, regexp_replace

# script inputs
opts = argparse.ArgumentParser()
# opts.add_argument('-n', type=int, default=50)
opts.add_argument('phenotype')

# parse command line
args = opts.parse_args()

# common variables
# phenotype = 'BMI'
phenotype = args.phenotype

# EC2 development localhost directories
ncbi_srcdir = 's3://dig-analysis-data/bin/magma/'
gene_pvalues_srcdir = 's3://dig-analysis-data/out/magma/step4GenePValues/'
out_dir = 's3://dig-analysis-data/out/magma/results'

# development localhost directories
# variant_srcdir = '/Users/mduby/Data/Broad/Magma/Snp/'
# out_dir = '/Users/mduby/Data/Broad/Magma/Out/Step1'

# localhost development localhost directories
# ncbi_srcdir = '/home/javaprog/Data/Broad/dig-analysis-data/bin/magma'
# gene_pvalues_srcdir = '/home/javaprog/Data/Broad/dig-analysis-data/out/magma/step4GenePValues'
# out_dir = '/home/javaprog/Data/Broad/dig-analysis-data/out/magma/results'

# ncbi schema
ncbi_schema = StructType([
    StructField("geneNcbiId", IntegerType(), True),
    StructField("chromosome", StringType(), True),
    StructField("start", IntegerType(), True),
    StructField("end", IntegerType(), True),
    StructField("direction", StringType(), True),
    StructField("gene", StringType(), True)
    ])

# this is the schema for reading the gene results file generated from magma
# use only one column since the delimiter is a variable number of spaces
# will split into fields after loading
gene_pvalue_schema = StructType(
    [
        StructField('generic', StringType(), nullable=False),
    ]
)

# open spark session
spark = SparkSession.builder.appName('magma01').getOrCreate()
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )
print("got Spark session of type {}".format(type(spark)))

# print
print("the ncbi input directory is: {}".format(ncbi_srcdir))
print("the gene pValues input directory is: {}".format(gene_pvalues_srcdir))
print("the output directory is: {}".format(out_dir))

# method to load the ncbi lookup file
def load_ncbi_lookup(input_dir):
    df = spark.read.csv('%s/NCBI37.3.gene.loc' % (input_dir), sep='\t', header=False, schema=ncbi_schema)

    # return
    return df

# method to load the phenotype ontology lookup file
def load_phenotype_ontology_lookup(input_dir):
    df = spark.read.csv('%s/PhenotypeOntologyMapping.tsv' % (input_dir), sep='\t', header=True)

    # return
    return df

# method to load the gene pValues files
def load_gene_pvalues(input_dir, phenotype):
    # df = spark.read.csv('%s/%s/genePValues.txt' % (input_dir, phenotype), sep='\\s+', ignoreLeadingWhiteSpace=True, header=True)
    # df = DataFrame(load('%s/%s/genePValues.txt' % (input_dir, phenotype), spacedelim=true))
    # rdd = spark.textFile('%s/%s/genePValues.txt' % (input_dir, phenotype)).map(line=>line.split("\\s+"))
    # df = spark.read.option("header","true")\
    #     .option("delimiter"," ")\
    #     .csv('%s/%s/genePValues.txt' % (input_dir, phenotype)) 

    # load the dataframe as one line due to dynamic space delimiter
    df_load = spark.read.csv('%s/%s/genePValues.txt' % (input_dir, phenotype), schema=gene_pvalue_schema, header=True)

    # now split the columns
    split_col = split(df_load.generic, '\\s+',)
    df = df_load.withColumn("geneNcbiId", split_col.getItem(0))\
        .withColumn("nParam", split_col.getItem(5))\
        .withColumn("subjects", split_col.getItem(6))\
        .withColumn("zStat", split_col.getItem(7))\
        .withColumn("pValue", split_col.getItem(8)) \
        .withColumn('phenotype', lit(phenotype))\

    # return
    return df

# load the gene pvalue file
df_pvalue = load_gene_pvalues(gene_pvalues_srcdir, phenotype)
df_pvalue.show()

# load the ontology file# load the ontology file
df_ontology = load_phenotype_ontology_lookup(ncbi_srcdir)
df_ontology = df_ontology \
    .select('PH', 'MONDO ID exact', 'EFO ID exact') \
    .withColumnRenamed('PH', 'phenotype') \
    .withColumnRenamed('MONDO ID exact', 'phenotypeMondoId') \
    .withColumnRenamed('EFO ID exact', 'phenotypeEfoId') 
df_ontology.show()

# filter the ontology by phenotype
df_ontology = df_ontology.filter(df_ontology.phenotype == phenotype)
df_ontology.show()

# join the ontology with the gene values
df_phenotype_pvalue = df_pvalue.join(df_ontology, on='phenotype', how='left_outer')
df_phenotype_pvalue.show()

# load the lookup file
df_ncbi = load_ncbi_lookup(ncbi_srcdir)
df_ncbi.show()

# join the two dataframes and add in rsIDs
df_export = df_phenotype_pvalue.join(df_ncbi, on='geneNcbiId', how='inner')
df_export.show()
df_export = df_export.select('gene', 'phenotype', 'geneNcbiId', 'nParam', 'subjects', 'zStat', 'pValue', 'phenotypeEfoId', 'phenotypeMondoId') \
    .withColumn('geneNcbiId', df_export['geneNcbiId'].cast(IntegerType())) \
    .withColumn('zStat', df_export['zStat'].cast(DoubleType())) \
    .withColumn('subjects', df_export['subjects'].cast(IntegerType())) \
    .withColumn('nParam', df_export['nParam'].cast(IntegerType())) \
    .withColumn('pValue', df_export['pValue'].cast(DoubleType()))
print("the loaded variant joined data frame has {} rows".format(df_export.count()))
df_export.show()

# write out the one tab delimited file
df_export \
        .orderBy(df_export.gene) \
        .write \
        .mode('overwrite') \
        .json('%s/%s' % (out_dir, phenotype))
print("wrote out the reconciled gene pvalue data frame to directory {}/{}".format(out_dir, phenotype))

# stop spark
# spark.stop()

